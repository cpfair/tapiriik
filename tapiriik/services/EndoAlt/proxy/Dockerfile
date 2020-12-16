FROM node:10-alpine

# Copy node application
COPY . /app
WORKDIR "/app"

RUN npm install --production

EXPOSE 8001

# Start application
ENTRYPOINT ["npm","run-script","server"]