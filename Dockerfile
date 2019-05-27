FROM python:3.6.8-alpine

RUN apk --update add bash curl

WORKDIR /usr/src/catiline
ADD . /usr/src/catiline

RUN pip3 install --user pipenv
ENV PATH="/root/.local/bin:${PATH}"
RUN pipenv install

EXPOSE 8080
CMD ./startup.sh
