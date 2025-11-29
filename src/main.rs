use reqwest;
use serde_json::Value;
use std::env;
use std::time::Duration;
use tokio_postgres::{Client, Error as PgError, NoTls};
use url::Url;

//its a comment


fn create_divar_url(
    lon1: f64,
    lat1: f64,
     lon2: f64,
    lat2: f64,
    category: &str,
    price: i64 ,
    recent_ads: Option<&str>,
) -> String {
    let mut url = Url::parse("https://api.divar.ir/v8/map-discovery/bbox/posts/count?").unwrap();

    url.query_pairs_mut()
        .append_pair("lon1", &lon1.to_string())
        .append_pair("lat1", &lat1.to_string())
        .append_pair("lon2", &lon2.to_string())
        .append_pair("lat2", &lat2.to_string())
        .append_pair("filters", &format!("category={}", category));

    if price != -1 {
        url.query_pairs_mut()
            .append_pair("filters", &format!("price=-{}", price));
    }

    if let Some(recent_ads_value) = recent_ads {
        url.query_pairs_mut()
            .append_pair("filters", &format!("recent_ads={}", recent_ads_value));
    }

    url.to_string().replace("%3D", "=")
}

async fn create_tables(client: &Client) -> Result<(), PgError> {
    client
        .batch_execute(
            "
        CREATE TABLE IF NOT EXISTS divar_data (
            date DATE DEFAULT CURRENT_DATE,
            lon1 DOUBLE PRECISION,
            lat1 DOUBLE PRECISION,
            lon2 DOUBLE PRECISION,
            lat2 DOUBLE PRECISION,
            category TEXT,
            price BIGINT,
            recent_ads TEXT,
            count INTEGER,
            PRIMARY KEY (date, lon1, lat1, lon2, lat2, category, price, recent_ads)
        );
        CREATE TABLE IF NOT EXISTS dollar (
            date BIGINT PRIMARY KEY,
            dollar_price INTEGER
        );
    ",
        )
        .await?;
    Ok(())
}

async fn fetch_and_insert_data(
    client: &Client,
    url: String,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
    category: &str,
    price: i64,
    recent_ads: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let response = http_client.get(&url).send().await?;
    let json: Value = response.json().await?;

    let count = json["count"].as_i64().unwrap_or(0) as i32;

    client
        .execute(
            "INSERT INTO divar_data (lon1, lat1, lon2, lat2, category, price, recent_ads, count) 
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &lon1,
                &lat1,
                &lon2,
                &lat2,
                &category,
                &price,
                &recent_ads.unwrap_or(""),
                &count,
            ],
        )
        .await?;

    println!("Inserted data for URL: {}, Count: {}", url, count);
    // tokio::time::sleep(Duration::from_millis(500)).await;

    Ok(())
}

// pub fn stock_cumulative_cache() -> Result<(), Box<dyn std::error::Error>> {
//     execute_query("DROP MATERIALIZED VIEW IF EXISTS mv_divar_apartment;", &[])?;
//     let q2 = "DROP MATERIALIZED VIEW IF EXISTS mv_divar_plotold;" ;
//     Ok(())
// }

fn build_conn_str() -> String {
    let host = env::var("PGHOST").unwrap_or_else(|_| "pgdk".to_string());
    let user = env::var("PGUSER").unwrap_or_else(|_| "dev".to_string());
    let password =
        env::var("PGPASSWORD").unwrap_or_else(|_| "vatanampareyetanameyiran".to_string());
    let db = env::var("PGDATABASE").unwrap_or_else(|_| "bourse".to_string());
    format!("host={} user={} password={} dbname={}", host, user, password, db)
}

async fn insert_dollar () -> Result<(), Box<dyn std::error::Error>> {
        let conn_str = build_conn_str();

    // Create async connection
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let http_client = reqwest::Client::builder()
    .timeout(Duration::from_secs(10))
    .build()?;

    
    let response = http_client.get("https://api.tgju.org/v1/market/indicator/summary-table-data/price_dollar_rl?lang=fa&order_dir=asc&start=0&length=600000&from=&to=&convert_to_ad=1").send().await?;
    let parsed_json: Value = response.json().await?;

    let mut insert_query = String::from("INSERT INTO dollar (date, dollar_price) VALUES ");
    let mut values: Vec<String> = Vec::new();

    
    if let Some(records) = parsed_json["data"].as_array() {
        for record in records {
            let date = record[6].as_str().ok_or("Missing or invalid date")?.replace("/", "").parse::<i64>().map_err(|_| "Failed to parse date")?;

            let dollar_price = record[3].as_str().ok_or("Missing or invalid dollar_price")?.replace(",", "").parse::<i64>().map_err(|_| "Failed to parse dollar price")?;

            // Format each value into a SQL-friendly format
            let value_str = format!("({}, {})", date, dollar_price);
            values.push(value_str);
        }
    }

    insert_query.push_str(&values.join(", "));
    insert_query.push_str(" ON CONFLICT (date) DO NOTHING;");
    client.execute(&insert_query,&[]).await?;
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn_str = build_conn_str();

    // Create async connection
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    create_tables(&client).await?;

    let locations = [
        (
            51.12108541489556,
            35.53440416028046,
            51.624314585103065,
            35.88361336225029,
        ),
        (
            51.48855091343077,
            32.519298780640995,
            51.80264908656426,
            32.745358440861224,
        ),
    ];

    let categories = ["apartment-sell", "plot-old"];
    let prices = [12000000000, 8000000000, 4000000000, -1];
    let recent_ads_options = [Some("1d"), Some("7d"), None];

    for (lon1, lat1, lon2, lat2) in locations {
        for category in &categories {
            for price in &prices {
                for recent_ads in &recent_ads_options {
                    let url =
                        create_divar_url(lon1, lat1, lon2, lat2, category, *price, *recent_ads);

                    if let Err(e) = fetch_and_insert_data(
                        &client,
                        url.clone(),
                        lon1,
                        lat1,
                        lon2,
                        lat2,
                        category,
                        *price,
                        *recent_ads,
                    )
                    .await
                    {
                        eprintln!("Error processing URL {}: {}", url, e);
                    }
                }
            }
        }
    }

    client
        .execute("DROP MATERIALIZED VIEW IF EXISTS mv_divar_apartment;", &[])
        .await?;
    client
        .execute("DROP MATERIALIZED VIEW IF EXISTS mv_divar_plotold;", &[])
        .await?;
    client
        .execute(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_divar_apartment AS
                select date,sum(count) from divar_data where 
                    price = 8000000000 and recent_ads = '7d'  and category = 'apartment-sell' 
                    group by date 
                    order by date ; ",
            &[],
        )
        .await?;

    client
        .execute(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_divar_plotold AS
                select date,sum(count) from divar_data where 
                    price = 8000000000 and recent_ads = '7d'  and category = 'plot-old' 
                    group by date 
                    order by date ; ",
            &[],
        )
        .await?;

    insert_dollar().await? ; 

    Ok(())

    
}
