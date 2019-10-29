FROM python:3.6


ADD requirements.txt /app/
WORKDIR /app
RUN pip install --upgrade pip && pip install gunicorn==19.9.0
RUN while read p; do pip install $p; done < requirements.txt;

COPY . /app
WORKDIR /app/scripts
EXPOSE 8085

ENV ZTF_USER=""
ENV ZTF_PASSWORD=""
ENV ZTF_HOST=""
ENV ZTF_PORT="5432"
ENV ZTF_DATABASE=""
ENV APP_WORKERS="2"
ENV APP_BIND="0.0.0.0"
ENV APP_PORT="8085"

ENV FEATURES_TABLE="features_v2"
ENV LATE_PROBABILITIES_TABLE="late_probabilities_v2"
ENV EARLY_PROBABILITIES_TABLE="stamp_classification"


CMD ["/bin/bash","entrypoint.sh"]
