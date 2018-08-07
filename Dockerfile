FROM node:8-alpine

LABEL maintainer="nettarkivet@nb.no"

COPY package.json yarn.lock /usr/src/app/
WORKDIR /usr/src/app

RUN apk add --no-cache git \
&& yarn install --production \
&& yarn cache clean

COPY . .

ENV DB_PORT=28015 \
    DB_HOST=localhost \
    DB_USER=admin \
    DB_PASSWORD='' \
    LOG_LEVEL=info

EXPOSE 3010

ENTRYPOINT ["/usr/local/bin/node", "index.js"]
