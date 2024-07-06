#[cfg(test)]
mod integration_tests {
    use std::collections::HashSet;

    use futures::StreamExt;
    use lazy_static::lazy_static;
    use rudis::{
        client::{RedisClient, RedisClientError},
        list::RList,
        map::RMap,
        set::RSet,
        RObjectAsync,
    };

    lazy_static! {
        static ref CLIENT: RedisClient = RedisClient::create("redis://127.0.0.1:6379").unwrap();
    }

    #[tokio::test]
    async fn test_list() -> Result<(), RedisClientError> {
        // given
        let name = "unique_alphabet";
        let rlist: RList<String> = CLIENT.get_list(name);

        // adds entries
        rlist.push_async("a".into()).await?;
        rlist.push_async("b".into()).await?;
        rlist.push_async("c".into()).await?;
        rlist.push_async("d".into()).await?;
        rlist.push_async("e".into()).await?;

        let size = rlist.size_async().await?;
        assert_eq!(size, 5);

        // gets by index
        let c = rlist.get_async(2).await?;
        assert_eq!(c, Some("c".into()));

        // removes entry
        rlist.remove_async("d".into()).await?;

        let entries = rlist.read_all_async().await?;
        assert_eq!(
            entries,
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "e".to_string()
            ]
        );

        // finds by index
        let index = rlist.find_index_async("b".into()).await?;
        assert_eq!(index, Some(1));

        // sets by index
        rlist.set_async(3, "d".into()).await?;

        let entries = rlist.read_all_async().await?;
        assert_eq!(
            entries,
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string()
            ]
        );

        // trims list
        rlist.trim_async(0, 2).await?;

        let entries = rlist.read_all_async().await?;
        assert_eq!(
            entries,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );

        // stream values
        let stream = rlist.stream().await?;

        let entries: Vec<String> = stream.collect().await;
        assert_eq!(
            entries,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );

        // clears list
        rlist.clear_async().await?;

        let exists = rlist.exists_async().await?;
        assert!(!exists);

        Ok(())
    }

    #[tokio::test]
    async fn test_map() -> Result<(), RedisClientError> {
        // given
        let name = "names";
        let rmap: RMap<String, String> = CLIENT.get_map(name);

        // adds entries
        rmap.insert_async("a".to_string(), "alex".to_string())
            .await?;
        rmap.insert_async("m".to_string(), "martha".to_string())
            .await?;

        let a = rmap.get_async("a".into()).await?;
        assert_eq!(a, Some("alex".to_string()));

        let m = rmap.get_async("m".into()).await?;
        assert_eq!(m, Some("martha".to_string()));

        let z = rmap.get_async("z".into()).await?;
        assert_eq!(z, None);

        // contains
        let has_a = rmap.contains_async("a".to_string()).await?;
        assert!(has_a);

        // get values
        let values = rmap.values_async().await?;
        assert_eq!(values, vec!["alex".to_string(), "martha".to_string()]);

        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> Result<(), RedisClientError> {
        // given
        let name = "alphabet";
        let rset: RSet<String> = CLIENT.get_set(name);

        // adds entries
        rset.add_all_async(&["a".into(), "b".into(), "c".into(), "d".into(), "e".into()])
            .await?;

        let size = rset.size_async().await?;
        assert_eq!(size, 5);

        // contains
        let has_a = rset.contains_async(&"a".into()).await?;
        assert!(has_a);

        let has_z = rset.contains_async(&"z".into()).await?;
        assert!(!has_z);

        // removes entry
        rset.remove_async(&"d".into()).await?;

        let entries = rset.read_all_async().await?;
        assert_eq!(
            entries,
            HashSet::from([
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "e".to_string()
            ])
        );

        // clears list
        rset.clear_async().await?;

        let exists = rset.exists_async().await?;
        assert!(!exists);

        Ok(())
    }
}
