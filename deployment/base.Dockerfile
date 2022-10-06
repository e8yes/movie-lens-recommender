FROM dokken/ubuntu-22.04

WORKDIR /home
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update

RUN apt install -y          \
    git                     \
    python3-pip             \
    libpq-dev               \
    protobuf-compiler-grpc

RUN pip3 install --upgrade  \
    psycopg2                \
    protobuf                \
    grpcio

ADD https://api.github.com/repos/e8yes/movie-lens-recommender/git/refs/heads/main version.json
RUN git clone https://github.com/e8yes/movie-lens-recommender.git

WORKDIR /home/movie-lens-recommender

RUN cd src/ingestion && ./generate_code.sh
