FROM python:3.8.10

# copy and install files
RUN pip3 install --upgrade pip

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libnspr4 libnss3 lsb-release xdg-utils libxss1 libdbus-glib-1-2 \
    curl unzip wget \
    xvfb

RUN mkdir usr/local/app
WORKDIR usr/local/app
COPY . .
RUN pip3 install -r requirements.txt

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONUNBUFFERED=1

CMD [ "python3", "./get_bbb_data.py" ]