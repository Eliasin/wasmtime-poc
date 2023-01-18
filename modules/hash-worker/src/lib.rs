use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

wit_bindgen_guest_rust::generate!("../../wit-bindgen/apis.wit");

struct Start;

export_apis!(Start);

#[derive(Deserialize)]
struct Request<'a> {
    data: &'a [u8],
    leading_zeros: u8,
}

#[derive(Serialize)]
struct Response<'a> {
    data: &'a [u8],
    leading_zeros: u8,
    nonce: u16,
}

fn digest_leading_zeroes(digest_bytes: &[u8]) -> u8 {
    let mut leading_zeros = 0;

    for byte in digest_bytes {
        let byte_leading_zeros = byte.leading_zeros();

        leading_zeros += byte_leading_zeros;

        if byte_leading_zeros != 8 {
            return leading_zeros as u8;
        }
    }

    leading_zeros as u8
}

fn handle_request<'a>(request: &'a Request) -> Response<'a> {
    let mut hasher = Md5::new();
    let mut nonce: u16 = 0;

    loop {
        hasher.update(request.data);
        hasher.update(nonce.to_ne_bytes());

        let digest = hasher.finalize_reset();

        if digest_leading_zeroes(digest.as_slice()) >= request.leading_zeros {
            let Request {
                data,
                leading_zeros,
            } = request;

            return Response {
                data,
                leading_zeros: *leading_zeros,
                nonce,
            };
        }
        nonce += 1;
    }
}

impl apis::Apis for Start {
    fn start(arg: Option<Vec<u8>>) -> Result<(), String> {
        let Some(arg) = arg else {
            return Err("hash-worker module expected argument".to_string());
        };

        let Ok(request) = postcard::from_bytes::<Request>(&arg) else {
           return Err("hash-worker encountered error when deserializing request".to_string()); 
        };

        let response = handle_request(&request);

        mqtt::publish(
            "hash/response",
            mqtt::QualityOfService::ExactlyOnce,
            false,
            &postcard::to_vec::<Response<'_>, 256>(&response)
                .map_err(|e| format!("hash-worker encountered error serializing response: {e}"))?,
        )
        .map_err(|e| format!("hash-worker encountered error while publishing response: {e}"))?;

        Ok(())
    }
}
