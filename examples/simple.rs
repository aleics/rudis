use rudis::client::{Client, ClientError};

#[tokio::main]
pub async fn main() -> Result<(), ClientError> {
    let client = Client::create("127.0.0.1:6379").await?;

    let response = client.ping().await?;
    println!("{:?}", response);

    Ok(())
}
