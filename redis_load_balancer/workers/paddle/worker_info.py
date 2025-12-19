

OCR_input_streams = [
    {
        "stream": "OCR:requests",
        "group": "OCR_consumers",
        "message_batch_size": 5,
        "claim_idle_time_ms": 100000,
        "claim_max_count": 10,
        "stream_block_time_ms": 5000,
        "stream_error_claim": "OCR:errors",
    },
    {
        "stream": "OCR:requests_large",
        "group": "OCR_consumers_large",
        "message_batch_size": 1,
        "claim_idle_time_ms": 100000,
        "claim_max_count": 10,
        "stream_block_time_ms": 5000,
        "stream_error_claim": "OCR:errors",
    }
    ]

OCR_result_streams = [{
    "stream": "OCR:responses",
    "group": "OCR_result",
    "message_batch_size": 1,
    "claim_idle_time_ms": 100000,
    "claim_max_count": 10,
    "stream_block_time_ms": 5000,
    "stream_error_claim": "OCR:errors",
}
]

payload_shape = {
    "image_path": str,
    "page_number": int,
    "total_pages": int,
    "result": dict,
}