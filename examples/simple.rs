use rudis::{client::RedisClient, list::RList, RObjectAsync, RudisError};

#[tokio::main]
pub async fn main() -> Result<(), RudisError> {
    let client = RedisClient::create("redis://127.0.0.1:6379")?;

    let response = client.is_connected()?;
    println!("is connected: {}", response);

    let rlist: RList<String> = client.get_list("my_list")?;
    rlist.push_async("a".into()).await?;
    rlist.push_async("b".into()).await?;
    rlist.push_async("c".into()).await?;
    rlist.push_async("d".into()).await?;

    let size = rlist.size_async().await?;
    println!("inserted {} elements", size);

    let index = rlist.find_index_async("c".into()).await?;
    println!("index of \"c\" is {:?}", index);

    let a = rlist.get_async(0).await?;
    println!("a {:?}", a);

    rlist.remove_async("c".into()).await?;

    let size = rlist.size_async().await?;
    println!("after remove there's {} elements", size);

    rlist.set_async(1, "z".into()).await?;
    println!("is this z? {:?}", rlist.get_async(1).await?);

    println!("listing all the elements:");
    for item in rlist.iter()? {
        println!("value {}", item);
    }

    rlist.clear_async().await?;

    Ok(())
}
