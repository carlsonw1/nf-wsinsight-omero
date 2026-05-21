FROM mambaorg/micromamba:1.5.10

USER root

RUN apt-get update && apt-get install -y \
    bash \
    ca-certificates \
    curl \
    jq \
    unzip \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN micromamba install -y -n base -c conda-forge \
    python=3.11 \
    omero-py \
    && micromamba clean --all --yes

# Bring a pinned OMERO.java bundle into the image so runtime import does not need internet
COPY OMERO.java-5.6.17-ice36.zip /opt/omero-java-cache/

RUN unzip -q /opt/omero-java-cache/OMERO.java-5.6.17-ice36.zip -d /opt/omero-java-cache

ENV PATH=/opt/conda/bin:$PATH

WORKDIR /workspace

CMD ["/bin/bash"]