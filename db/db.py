create table bittensor_textembedding_table (
    Primary Key (run_name, miner_id,timestamp),
    score NUMERIC(6, 8),
    loss NUMERIC(6, 8),
    top1_recall NUMERIC(6, 8),
    top3_recall NUMERIC(6, 8),
)


loss, top1_recall, top3_recall