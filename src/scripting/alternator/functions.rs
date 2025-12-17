use super::alternator_error::{AlternatorError, AlternatorErrorKind};
use super::context::Context;
use aws_sdk_dynamodb::types::AttributeValue;
use rune::runtime::{Object, Ref, Shared};
use rune::Value;
use std::ops::Deref;

fn to_n_attribute_value(v: Value) -> AttributeValue {
    match v {
        Value::Integer(i) => AttributeValue::N(i.to_string()),
        Value::String(s) => AttributeValue::N(s.borrow_ref().unwrap().to_string()),
        _ => AttributeValue::N(format!("{:?}", v)),
    }
}

fn get_scalar_type(object: Shared<Object>) -> aws_sdk_dynamodb::types::ScalarAttributeType {
    if let Some(Value::String(s)) = object.borrow_ref().unwrap().get("type") {
        match s.borrow_ref().unwrap().as_str() {
            "N" => aws_sdk_dynamodb::types::ScalarAttributeType::N,
            "S" => aws_sdk_dynamodb::types::ScalarAttributeType::S,
            "B" => aws_sdk_dynamodb::types::ScalarAttributeType::B,
            _ => aws_sdk_dynamodb::types::ScalarAttributeType::S,
        }
    } else {
        aws_sdk_dynamodb::types::ScalarAttributeType::S
    }
}

#[rune::function(instance)]
pub async fn create_table(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    params: Object,
) -> Result<(), AlternatorError> {
    let client = ctx.client.as_ref().unwrap();
    let pk_name = match params.get("primary_key") {
        Some(Value::String(s)) => s.borrow_ref().unwrap().to_string(),
        Some(Value::Object(o)) => {
            if let Some(Value::String(s)) = o.borrow_ref().unwrap().get("name") {
                s.borrow_ref().unwrap().to_string()
            } else {
                "pk".to_string()
            }
        }
        _ => "pk".to_string(),
    };
    let pk_type = match params.get("primary_key") {
        Some(Value::Object(o)) => get_scalar_type(o.clone()),
        _ => aws_sdk_dynamodb::types::ScalarAttributeType::S,
    };

    let sk_name = match params.get("sort_key") {
        Some(Value::String(s)) => s.borrow_ref().unwrap().to_string(),
        Some(Value::Object(o)) => {
            if let Some(Value::String(s)) = o.borrow_ref().unwrap().get("name") {
                s.borrow_ref().unwrap().to_string()
            } else {
                "sk".to_string()
            }
        }
        _ => "sk".to_string(),
    };

    let sk_type = match params.get("sort_key") {
        Some(Value::Object(o)) => get_scalar_type(o.clone()),
        _ => aws_sdk_dynamodb::types::ScalarAttributeType::S,
    };

    let pk_schema = aws_sdk_dynamodb::types::KeySchemaElement::builder()
        .attribute_name(pk_name.clone())
        .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
        .build()
        .unwrap();

    let sk_schema = aws_sdk_dynamodb::types::KeySchemaElement::builder()
        .attribute_name(sk_name.clone())
        .key_type(aws_sdk_dynamodb::types::KeyType::Range)
        .build()
        .unwrap();

    let pk_def = aws_sdk_dynamodb::types::AttributeDefinition::builder()
        .attribute_name(pk_name)
        .attribute_type(pk_type)
        .build()
        .unwrap();

    let sk_def = aws_sdk_dynamodb::types::AttributeDefinition::builder()
        .attribute_name(sk_name)
        .attribute_type(sk_type)
        .build()
        .unwrap();

    client
        .create_table()
        .table_name(table_name.deref())
        .set_key_schema(Some(vec![pk_schema, sk_schema]))
        .set_attribute_definitions(Some(vec![pk_def, sk_def]))
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .map_err(|e| AlternatorError::new(AlternatorErrorKind::Error(e.to_string())))
        .ok();

    Ok(())
}

#[rune::function(instance)]
pub async fn delete_table(ctx: Ref<Context>, table_name: Ref<str>) -> Result<(), AlternatorError> {
    let client = ctx.client.as_ref().unwrap();
    client
        .delete_table()
        .table_name(table_name.deref())
        .send()
        .await
        .map_err(|e| AlternatorError::new(AlternatorErrorKind::Error(e.to_string())))?;
    Ok(())
}

#[rune::function(instance)]
pub async fn put_item(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    params: Object,
) -> Result<(), AlternatorError> {
    let client = ctx.client.as_ref().unwrap();

    let mut builder = client.put_item().table_name(table_name.deref());
    for (key, value) in params.iter() {
        let attr_value = to_n_attribute_value(value.clone());
        builder = builder.item(key.deref(), attr_value);
    }
    builder
        .send()
        .await
        .map_err(|e| AlternatorError::new(AlternatorErrorKind::Error(e.to_string())))?;
    Ok(())
}

#[rune::function(instance)]
pub async fn alternator_get_many_validate(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    pk: Value,
    max_limit: Value,
    expected_rows_num: u64,
) -> Result<Vec<String>, AlternatorError> {
    let client = ctx.client.as_ref().unwrap();

    let limit = match max_limit {
        Value::Integer(i) => i as i32,
        Value::String(s) => s.borrow_ref().unwrap().parse::<i32>().unwrap(),
        _ => 1,
    };

    let result = client
        .query()
        .table_name(table_name.deref())
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", to_n_attribute_value(pk))
        .limit(limit)
        .send()
        .await
        .map_err(|e| AlternatorError::new(AlternatorErrorKind::Error(e.to_string())))?;

    if let Some(items) = result.items {
        let output: Vec<String> = items
            .iter()
            .filter_map(|item| {
                if let Some(AttributeValue::N(ck)) = item.get("ck") {
                    Some(ck.clone())
                } else {
                    None
                }
            })
            .collect();
        assert!(output.len() as u64 == expected_rows_num);
        return Ok(output);
    }
    Ok(vec![])
}

#[rune::function(instance)]
pub async fn alternator_count_validate(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    pk: Value,
    expected_rows_num: u64,
) -> Result<i64, AlternatorError> {
    let client = ctx.client.as_ref().unwrap();

    let result = client
        .query()
        .table_name(table_name.deref())
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", to_n_attribute_value(pk))
        .select(aws_sdk_dynamodb::types::Select::Count)
        .send()
        .await
        .map_err(|e| AlternatorError::new(AlternatorErrorKind::Error(e.to_string())))?;

    assert!(result.count as u64 == expected_rows_num);
    Ok(result.count as i64)
}
