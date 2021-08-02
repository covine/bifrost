FROM nginx:latest

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' >/etc/timezone

COPY ./server/cert.pem /etc/bifrost/certs/cert.pem
COPY ./server/key.pem /etc/bifrost/certs/key.pem

COPY ./nginx.conf /etc/nginx/nginx.conf

EXPOSE 1883 8883