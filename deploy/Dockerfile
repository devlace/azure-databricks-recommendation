# Use Official Microsoft Azure CLI image
FROM continuumio/miniconda3:4.5.4

# Install any needed packages specified in requirements.txt
RUN apt-get update
RUN apt-get install -y autoconf automake build-essential libtool python-dev jq

# Set the working directory to /
WORKDIR /
# Copy the directory contents into the container at /
COPY . /

RUN make requirements

CMD ["make", "deploy"]


