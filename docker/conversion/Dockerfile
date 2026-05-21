FROM mambaorg/micromamba:1.5.10

USER root

RUN apt-get update && apt-get install -y \
    bash \
    ca-certificates \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN micromamba install -y -n base -c ome -c conda-forge \
    bioformats2raw \
    raw2ometiff \
    blosc \
    && micromamba clean --all --yes

ENV PATH=/opt/conda/bin:$PATH
ENV LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

WORKDIR /workspace

CMD ["/bin/bash"]