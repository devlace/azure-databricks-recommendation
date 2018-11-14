# Use Official Microsoft Azure CLI image
FROM continuumio/miniconda3:4.5.4

# Set the working directory to /
WORKDIR /
# Copy the directory contents into the container at /
COPY . /

# Install any needed packages specified in requirements.txt
RUN apt-get update \
    && apt-get install -y autoconf automake build-essential libtool python-dev jq \
    && make requirements \
    && chmod +x -R /deploy

CMD ["make", "deploy"]
