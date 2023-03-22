FROM python:3.10
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./group_into_batches.py ./group_into_batches.py
CMD ["python3","./group_into_batches.py"]
