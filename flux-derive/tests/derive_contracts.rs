use flux::{AggregateRoot as _, Entity as _, RelationKind};
use flux_derive::{AggregateRoot, Entity, MongoEmbedded, MongoEntity, SqlEntity, SqlServerEntity};
use flux_mongodb::{MongoEmbedded as _, MongoEntity as _, MongoObjectId};
use flux_postgres::SqlEntity as _;
use flux_sqlserver::SqlServerEntity as _;
use mongodb::bson::oid::ObjectId;
use uuid::Uuid;

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "order_items"]
struct OrderItem {
    #[primary_key]
    item_oid: Uuid,
    order_oid: Uuid,
    product_name: String,
    quantity: i32,
}

#[derive(Clone, Debug, Entity, SqlServerEntity)]
#[table_name = "categories"]
struct Category {
    #[primary_key]
    category_id: i64,
    name: String,
}

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "generated_products"]
struct GeneratedProduct {
    #[primary_key]
    #[generated_id]
    product_id: i64,
    name: String,
}

#[derive(Clone, Debug, Entity, SqlServerEntity)]
#[table_name = "sqlserver_order_items"]
struct SqlServerOrderItem {
    #[primary_key]
    item_oid: Uuid,
    order_oid: Uuid,
    product_name: String,
}

#[derive(Clone, Debug, Entity, SqlServerEntity, AggregateRoot)]
#[table_name = "sqlserver_orders"]
struct SqlServerOrder {
    #[primary_key]
    order_oid: Uuid,
    customer_name: String,

    #[has_many(
        foreign_key = "order_oid",
        references = "order_oid",
        on_replace = "delete_missing",
        cascade_delete
    )]
    items: Vec<SqlServerOrderItem>,
}

#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "orders"]
struct Order {
    #[primary_key]
    order_oid: Uuid,
    customer_name: String,

    #[has_many(
        foreign_key = "order_oid",
        references = "order_oid",
        on_replace = "delete_missing",
        cascade_delete
    )]
    items: Vec<OrderItem>,
}

#[derive(Clone, Debug, Entity, MongoEntity)]
#[collection_name = "customers"]
struct Customer {
    #[primary_key]
    id: MongoObjectId,
    name: String,
    age: i32,
    active: Option<bool>,
}

#[derive(Clone, Debug, Entity, MongoEntity)]
#[collection_name = "mongo_order_items"]
struct MongoOrderItem {
    #[primary_key]
    id: MongoObjectId,
    order_id: MongoObjectId,
    product_name: String,
}

#[derive(Clone, Debug, Entity, MongoEntity, AggregateRoot)]
#[collection_name = "mongo_orders"]
struct MongoOrder {
    #[primary_key]
    id: MongoObjectId,
    customer_name: String,

    #[has_many(
        foreign_key = "order_id",
        references = "id",
        on_replace = "unlink_missing"
    )]
    items: Vec<MongoOrderItem>,
}

#[derive(Clone, Debug, PartialEq, MongoEmbedded)]
struct EmbeddedLine {
    sku: String,
    quantity: i32,
}

#[derive(Clone, Debug, Entity, MongoEntity)]
#[collection_name = "mongo_carts"]
struct MongoCart {
    #[primary_key]
    id: MongoObjectId,
    customer_name: String,
    lines: Vec<EmbeddedLine>,
}

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "user_profiles"]
struct UserProfile {
    #[primary_key]
    profile_oid: Uuid,
    user_oid: Uuid,
    bio: String,
}

#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "users"]
struct User {
    #[primary_key]
    user_oid: Uuid,
    name: String,

    #[has_one(foreign_key = "user_oid", references = "user_oid")]
    profile: Option<UserProfile>,
}

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "courses"]
struct Course {
    #[primary_key]
    course_oid: Uuid,
    title: String,
}

#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "students"]
struct Student {
    #[primary_key]
    student_oid: Uuid,
    name: String,

    #[many_to_many(
        join_table = "enrollments",
        source_key = "student_oid",
        target_key = "course_oid",
        target_primary_key = "course_oid",
        on_replace = "delete_missing"
    )]
    courses: Vec<Course>,
}

#[test]
fn derives_sql_entity_contract() {
    assert_eq!(Order::table_name(), "orders");
    assert_eq!(Order::primary_key(), "order_oid");
    assert_eq!(Order::fields(), &["order_oid", "customer_name"]);

    let order = Order {
        order_oid: Uuid::new_v4(),
        customer_name: "Ada".to_string(),
        items: Vec::new(),
    };

    assert_eq!(order.id(), &order.order_oid);
    assert!(order.items.is_empty());
    assert_eq!(order.to_insert_params().len(), 2);
    assert_eq!(order.to_update_params().len(), 1);
}

#[test]
fn derives_aggregate_metadata_and_includes() {
    let relations = Order::relations();

    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].name, "items");
    assert_eq!(relations[0].kind, RelationKind::HasMany);
    assert_eq!(relations[0].foreign_key, Some("order_oid"));
    assert_eq!(relations[0].references, Some("order_oid"));
    assert_eq!(Order::items().name, "items");
}

#[test]
fn derives_one_to_one_and_many_to_many_metadata() {
    let user_relations = User::relations();
    assert_eq!(user_relations.len(), 1);
    assert_eq!(user_relations[0].name, "profile");
    assert_eq!(user_relations[0].kind, RelationKind::HasOne);
    assert_eq!(User::profile().name, "profile");

    let student_relations = Student::relations();
    assert_eq!(student_relations.len(), 1);
    assert_eq!(student_relations[0].name, "courses");
    assert_eq!(student_relations[0].kind, RelationKind::ManyToMany);
    assert_eq!(student_relations[0].join_table, Some("enrollments"));
    assert_eq!(Student::courses().name, "courses");

    let user = User {
        user_oid: Uuid::new_v4(),
        name: "Ada".to_string(),
        profile: None,
    };
    let student = Student {
        student_oid: Uuid::new_v4(),
        name: "Ada".to_string(),
        courses: Vec::new(),
    };

    assert!(user.profile.is_none());
    assert!(student.courses.is_empty());
}

#[test]
fn derives_mongo_entity_contract() {
    let customer = Customer {
        id: MongoObjectId(ObjectId::new()),
        name: "Ada".to_string(),
        age: 36,
        active: Some(true),
    };

    let document = customer.to_document().expect("document");
    assert!(document.get_object_id("_id").is_ok());
    assert_eq!(document.get_str("name").expect("name"), "Ada");
    assert_eq!(document.get_i32("age").expect("age"), 36);

    let restored = Customer::from_document(document).expect("restored customer");
    assert_eq!(restored.id(), customer.id());
    assert_eq!(restored.name, customer.name);
    assert_eq!(restored.active, Some(true));
}

#[test]
fn derives_mongo_aggregate_contract() {
    let order = MongoOrder {
        id: MongoObjectId(ObjectId::new()),
        customer_name: "Ada".to_string(),
        items: Vec::new(),
    };

    assert_eq!(MongoOrder::collection_name(), "mongo_orders");
    assert_eq!(MongoOrder::relations().len(), 1);
    assert_eq!(MongoOrder::items().name, "items");
    assert!(order.items.is_empty());
}

#[test]
fn derives_mongo_embedded_contract() {
    let line = EmbeddedLine {
        sku: "SKU-1".to_string(),
        quantity: 2,
    };
    let document = line.to_document().expect("embedded document");
    assert_eq!(document.get_str("sku").expect("sku"), "SKU-1");
    assert_eq!(document.get_i32("quantity").expect("quantity"), 2);

    let cart = MongoCart {
        id: MongoObjectId(ObjectId::new()),
        customer_name: "Ada".to_string(),
        lines: vec![line.clone()],
    };

    let document = cart.to_document().expect("cart document");
    let lines = document.get_array("lines").expect("lines");
    assert_eq!(lines.len(), 1);

    let restored = MongoCart::from_document(document).expect("restored cart");
    assert_eq!(restored.lines, vec![line]);
}

#[test]
fn derives_sqlserver_entity_contract() {
    let category = Category {
        category_id: 10,
        name: "Hardware".to_string(),
    };

    assert_eq!(Category::table_name(), "categories");
    assert_eq!(Category::primary_key(), "category_id");
    assert_eq!(Category::fields(), &["category_id", "name"]);
    assert_eq!(category.id(), &10);
    assert_eq!(category.to_insert_params().len(), 2);
    assert_eq!(category.to_update_params().len(), 1);
}

#[test]
fn derives_generated_id_contract() {
    let mut product = GeneratedProduct {
        product_id: 0,
        name: "Keyboard".to_string(),
    };

    assert!(!product.has_id());
    product.set_id(42);
    assert!(product.has_id());
    assert_eq!(product.id(), &42);
    assert_eq!(GeneratedProduct::fields(), &["product_id", "name"]);
    assert_eq!(product.to_update_params().len(), 1);
}

#[test]
fn derives_sqlserver_aggregate_contract() {
    let order = SqlServerOrder {
        order_oid: Uuid::new_v4(),
        customer_name: "Ada".to_string(),
        items: Vec::new(),
    };

    assert_eq!(SqlServerOrder::table_name(), "sqlserver_orders");
    assert_eq!(SqlServerOrder::relations().len(), 1);
    assert_eq!(SqlServerOrder::items().name, "items");
    assert!(order.items.is_empty());
}
