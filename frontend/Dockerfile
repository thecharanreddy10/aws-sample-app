FROM nginxinc/nginx-unprivileged:1.27-alpine

WORKDIR /usr/share/nginx/html

COPY index.html /usr/share/nginx/html/index.html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080

CMD ["/bin/sh", "-c", "echo '[frontend] startup complete'; nginx -g 'daemon off;'"]
