FROM python:3.10-alpine


RUN apk --no-cache add bash postgresql13-client && \
    addgroup user && \
    adduser -s /bin/bash -D -G user user

WORKDIR /padmy

ADD requirements.txt /padmy
#
RUN apk upgrade expat && \
    apk --no-cache add --virtual build-dependencies gcc make musl-dev libffi-dev openssl-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del build-dependencies && \
    rm requirements.txt

RUN chown -R user:user /padmy

ADD ../../run.py .
ADD ../../padmy padmy/

EXPOSE 5555

USER user

ENTRYPOINT ["python", "run.py"]
