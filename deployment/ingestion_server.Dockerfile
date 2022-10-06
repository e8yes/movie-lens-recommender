FROM daviesx/jomaffeda-base:0.1

WORKDIR /home/movie-lens-recommender

CMD [ "python3",                                                    \
    "-m",                                                           \
    "src.ingestion.main_ingestion_server",                          \
    "--grpc_port=50051",                                            \
    "--postgres_host=127.0.0.1",                                    \
    "--postgres_password=0f21e4cd-44f8-48ab-b112-62030d7f7df1" ]
