FROM python:3

EXPOSE 3306

LABEL maintainer="Gabe Weiss <gweiss@google.com>"

COPY ./app /app

RUN pip install -r /app/requirements.txt

CMD [ "python", "/app/hammer.py" ]

