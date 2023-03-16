use anyhow::Result;
use chrono::Local;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::error::S3Error;


use log::{debug, error, info, log_enabled, Level};

pub struct Uploader {}

impl Uploader {
    pub fn upload_retry(&self, local_file: &str) -> anyhow::Result<()> {
        // return Ok(())
        info!("upload s3 file {}", local_file);
        for _ in 0..100 {
            // 300s => 5minutes
            match self.upload(local_file) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    error!("upload s3 failed {:?}", e);
                    std::thread::sleep(std::time::Duration::from_secs(3));
                }
            }
        }
        return anyhow::bail!("upload failed");
    }

    pub fn upload(&self, local_file: &str) -> anyhow::Result<()> {
        let start = Local::now().timestamp_millis();
     
        let file_name = local_file.split("/").last().unwrap();
        // 20221012_14_01.parquet 这种格式
        let names = file_name.split("_").collect::<Vec<_>>();
        let s3_key = if names.len() == 3 {
            let mut s3_key = names.join("/");
            let extend = format!("_{}", Local::now().timestamp_millis());
            s3_key.extend(extend.chars());
            s3_key 
        } else {
            // let c = format!("file_name is not support {:?}", local_file);
            panic!("file_name is not support");
        };

        info!("start to upload file_name {}", s3_key.as_str());
        let name = std::env::var("s3.bucketname")?;
        let region = std::env::var("AWS_REGION")?;
        let bucket = Bucket::new(
            name.as_str(),
            region.as_str().parse()?,
            // Credentials are collected from environment, config, profile or instance metadata
            Credentials::default()?,
        )?;

        let s3_path = s3_key.as_str();
        let content = std::fs::read(s3_path)?;
        use futures::executor;
    
        let response_data = bucket.put_object(s3_path, content.as_slice());
        let response_data = executor::block_on(response_data)?;
        if response_data.status_code() != 200 {
            let data = String::from_utf8_lossy(response_data.bytes());
            error!("start to upload file_name {} status_code == {} response {}", s3_key.as_str(), response_data.status_code(), data);
            anyhow::bail!("status_code is != 200");
        }
        let end = Local::now().timestamp_millis();
        info!(" upload file_name {} cost {} ms", s3_key.as_str(), end - start);
        return Ok(());
    }
}
