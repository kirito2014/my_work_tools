# nginx-error-page

Yet another error-page for nginx. Clone this repo and use https://github.com/tarampampam/error-pages to generate your own error pages.

This theme is modified from tarampampam/error-pages/templates/connections. Thanks to the author for the inspiration.

## Final result

Demo [here](https://shevonkuan.github.io/nginx-error-page/)

![alt text](light.gif)
![alt text](dark.gif)

## Known issues

-   Due to significant differences in the implementation of the `mix-blend-mode: hardlight` CSS property between Safari and Chrome, large color blocks appear instead of gradients in Safari.
-   It consumes a lot of GPU computing resources, and I don't know what to do. Seeking guidance from experts.
-   For ease of integration with nginx to record actual request information, nginx is used to string replacement (see the instructions in the usage section).

## Features

-   [x] Support multiple languages
-   [x] Support multiple platforms
-   [x] Support dark mode
-   [x] Support showing nginx request information
-   [x] Rich interactive effects
-   [x] Highly customizable
-   [x] No additional assets required
-   [x] Lightweight and fast

## Usage

### Use as an template for [tarampampam/error-pages](https://github.com/tarampampam/error-pages)

1. Download `main.html`
2. Clone [tarampampam/error-pages](https://github.com/tarampampam/error-pages)
3. Add `main.html` to `./error-pages/templates/` directory.
4. Follow the instructions in the [tarampampam/error-pages](https://github.com/tarampampam/error-pages)

### Use static files for nginx

1. Fork this repo.
2. Inintialize the submodules.
    ```bash
    git submodule update --init --recursive
    ```
3. Edit the `index.html` file in the `src` directory to customize your error page.(optional)
4. Use github Actions to generate static files.
5. Download the generated static files from the `gh-pages` branch or the artifact of the Actions.
6. Copy the static files to the directory eg. `/var/www/error-pages/`.
7. Locate to the NGINX config directory, usually `/etc/nginx/` or `/usr/local/nginx/conf/`.
8. Add a config file `error_pages.conf` in any directory, eg. `/etc/nginx/func.d/error_pages.conf`.(Ensure your nginx was built with `ngx_http_sub_module` in order to replace the strings listed below. If you use RHEL (including fedora, centos, etc.), you can enable the copr repository `dnf copr enable @shevon/nginx` to get the latest nginx version with the `ngx_http_sub_module` module.)

    ```nginx
    # error_pages.conf
    error_page 400 401 403 404 405 407 408 409 410 411 412 413 416 418 429 500 502 503 504 505 @error_page;

    location @error_page {
        root /var/www/error-pages/;
        set $error_file /$status.html;
        rewrite ^ $error_file break;
        sub_filter '%time_local%' '$time_local';
        sub_filter '%request%' '$request';
        sub_filter '%request_id%' '$request_id';
        sub_filter '%remote_addr%' '$remote_addr';
        sub_filter '%host%' '$host';
        sub_filter '%http_referer%' '$http_referer';
        sub_filter '%request_time%' '$request_time';
        sub_filter '%upstream_response_time%' '$upstream_response_time';
        sub_filter '%http_user_agent%' '$http_user_agent';
        sub_filter '%server%' 'nginx/$nginx_version';
    }
    ```

9. Include the config file in the `server block to replace its error pages`

    ```nginx
    # nginx.conf
    # For more information on configuration, see:
    #   * Official English Documentation: http://nginx.org/en/docs/
    #   * Official Russian Documentation: http://nginx.org/ru/docs/

    user root;
    worker_processes auto;
    error_log /var/log/nginx/error.log;
    pid /run/nginx.pid;

    # Load dynamic modules. See /usr/share/doc/nginx/README.dynamic.
    include /usr/share/nginx/modules/*.conf;

    events {
        worker_connections 1024;
    }

    http {
        sendfile on;
        tcp_nopush on;
        tcp_nodelay on;
        keepalive_timeout 65;
        types_hash_max_size 4096;

        include /etc/nginx/mime.types;
        include brotli_params;
        default_type application/octet-stream;

        include /etc/nginx/conf.d/*.conf;
        server {
            listen 443 ssl;
            http2 on;
            server_name shevon.is-a.dev;
            root /var/www/shevon/public;
            include ssl/shevon.is-a.dev;
            include /etc/nginx/func.d/error_pages.conf; # add this line
            location / {
                root /var/www/shevon/public;
            }
        }
    }
    ```
