FROM node:current-alpine

LABEL maintainer="nettarkivet@nb.no"

COPY package.json yarn.lock /usr/src/app/
WORKDIR /usr/src/app

RUN yarn install --production \
&& yarn cache clean

COPY . .

ENV DB_PORT=28015 \
    DB_HOST=localhost \
    DB_USER=admin \
    DB_PASSWORD='' \
    CREATE_DB_NAME='maalfrid' \
    CREATE_DB_USER='maalfrid' \
    CREATE_DB_PASSWORD='' \
    LOG_LEVEL=info

EXPOSE 3010

USER node

ENTRYPOINT ["/usr/local/bin/node", "index.js"]
