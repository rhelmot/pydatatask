"""This module contains repositories and other classes for interacting with S3-compatible bucket stores."""

from typing import Dict, Literal, Optional, overload
import io

from types_aiobotocore_s3.client import S3Client
import botocore.exceptions

from pydatatask import task as taskmodule
from pydatatask.host import LOCAL_HOST, Host
from pydatatask.session import Ephemeral
from pydatatask.utils import AReadText, AWriteText

from .base import BlobRepository, YamlMetadataRepository, job_getter


class S3BucketBinaryWriter:
    """A class for streaming (or buffering) byte data to be written to an `S3BucketRepository`."""

    def __init__(self, repo: "S3BucketRepository", job: str):
        self.repo = repo
        self.job = job
        self.buffer = io.BytesIO()
        super().__init__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close and flush the data to the bucket."""
        self.buffer.seek(0, io.SEEK_END)
        size = self.buffer.tell()
        self.buffer.seek(0, io.SEEK_SET)
        await self.repo.client.put_object(
            Bucket=self.repo.bucket,
            Key=self.repo.object_name(self.job),
            Body=self.buffer,
            ContentLength=size,
            ContentType=self.repo.mimetype,
        )

    async def write(self, data: bytes):
        """Write some data to the stream."""
        self.buffer.write(data)


class S3BucketReader:
    """A class for streaming byte data from an `S3BucketRepository`."""

    def __init__(self, body):
        self.body = body

    async def close(self):
        """Close and release the stream."""
        self.body.close()

    async def read(self, n=None):  # pylint: disable=unused-argument :(
        """Read the entire body of the blob.

        Due to API limitations, we can't read less than that at once...
        """
        return await self.body.read()

    async def __aenter__(self):
        await self.body.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.body.__aexit__(exc_type, exc_val, exc_tb)


class S3BucketInfo:
    """The data structure returned from :meth:`S3BucketRepository.info`.

    :ivar uri: The s3 URI of the current job's resource, e.g. ``s3://bucket/prefix/job.ext``. ``str(info)`` will also
               return this.
    :ivar endpoint: The URL of the API server providing the S3 interface.
    :ivar bucket: The name of the bucket objects are stored in.
    :ivar prefix: How to prefix an object name such that it will fit into this repository.
    :ivar suffix: How to suffix an object name such that it will fit into this repository.
    """

    def __init__(self, endpoint: str, uri: str, bucket: str, prefix: str, suffix: str):
        self.endpoint = endpoint
        self.uri = uri
        self.prefix = prefix
        self.suffix = suffix
        self.bucket = bucket

    def __str__(self):
        return self.uri


class S3BucketRepository(BlobRepository):
    """A repository where keys are paths in a S3 bucket.

    Provides a streaming interface to the corresponding blobs.
    """

    def __init__(
        self,
        client: Ephemeral[S3Client],
        bucket: str,
        prefix: str = "",
        suffix: str = "",
        mimetype: str = "application/octet-stream",
        endpoints: Optional[Dict[Optional[Host], str]] = None,
    ):
        """
        :param client: A callable returning an aiobotocore S3 client connected and authenticated to the server you wish
                       to store things on.
        :param bucket: The name of the bucket from which to load and store.
        :param prefix: A prefix to put on the job name before translating it into a bucket path. If this is meant to be
                       a directory name it should end with a slash character.
        :param suffix: A suffix to put on the job name before translating it into a bucket path. If this is meant to
                          be a file extension it should start with a dot.
        :param mimetype: The MIME type to set the content when adding data.
        :param incluster_endpoint: Optional: An endpoint URL to provide as the result of info() queries instead of
                                   extracting the URL from ``client``.
        """
        self._client = client
        self.bucket = bucket
        self.prefix = prefix
        self.suffix = suffix
        self.mimetype = mimetype
        self.endpoints = endpoints or {}

    @property
    def client(self):
        """The aiobotocore S3 client.

        This will raise an error if the client comes from a session which is not opened.
        """
        return self._client()

    def __repr__(self):
        return f"<{type(self).__name__} {self.bucket}/{self.prefix}*{self.suffix}>"

    async def contains(self, item):
        try:
            await self.client.head_object(Bucket=self.bucket, Key=self.object_name(item))
        except botocore.exceptions.ClientError:
            return False
        else:
            return True

    async def unfiltered_iter(self):
        paginator = self.client.get_paginator("list_objects")
        async for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(self.suffix):
                    yield obj["Key"][len(self.prefix) : -len(self.suffix) if self.suffix else None]

    async def validate(self):
        try:
            await self.client.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            if "404" in str(e):
                await self.client.create_bucket(Bucket=self.bucket)
            else:
                raise

    def object_name(self, job):
        """Return the object name for the given job."""
        return f"{self.prefix}{job}{self.suffix}"

    def get_endpoint(self, host: Host) -> str:
        """Given a host, return the endpoint that makes sense."""
        if host == LOCAL_HOST:
            return self.client._endpoint.host  # type: ignore
        endpoint = self.endpoints.get(host, None)
        if endpoint is None:
            endpoint = self.endpoints.get(None, None)
        if endpoint is None:
            raise ValueError(f"No endpoint specified from host {host}")
        return endpoint

    @overload
    async def open(self, job: str, mode: Literal["r"]) -> AReadText:
        ...

    @overload
    async def open(self, job: str, mode: Literal["w"]) -> AWriteText:
        ...

    @overload
    async def open(self, job: str, mode: Literal["rb"]) -> S3BucketReader:
        ...

    @overload
    async def open(self, job: str, mode: Literal["wb"]) -> S3BucketBinaryWriter:
        ...

    @job_getter
    async def open(self, job, mode="r"):
        if not self.is_valid_job_id(job):
            raise KeyError(job)
        if mode == "wb":
            return S3BucketBinaryWriter(self, job)
        elif mode == "w":
            return AWriteText(S3BucketBinaryWriter(self, job))
        elif mode == "rb":
            return S3BucketReader((await self.client.get_object(Bucket=self.bucket, Key=self.object_name(job)))["Body"])
        elif mode == "r":
            return AReadText(
                S3BucketReader((await self.client.get_object(Bucket=self.bucket, Key=self.object_name(job)))["Body"])
            )
        else:
            raise ValueError(mode)

    async def delete(self, job):
        await self.client.delete_object(Bucket=self.bucket, Key=self.object_name(job))

    async def template(self, job: str, task: taskmodule.Task, kind: taskmodule.LinkKind) -> taskmodule.TemplateInfo:
        if kind == taskmodule.LinkKind.InputFilepath:
            input_filepath = task.mktemp(job)
            endpoint = self.get_endpoint(task.host)
            preamble = task.mk_http_get(
                input_filepath, f"{endpoint}/api/v1/{self.bucket}/{self.object_name(job)}", headers={}
            )  # BT: Fix the headers
            return taskmodule.TemplateInfo(input_filepath, preamble=preamble)
        if kind == taskmodule.LinkKind.OutputFilepath:
            output_filepath = task.host.mktemp(job)
            endpoint = self.get_endpoint(task.host)
            epilogue = task.mk_http_post(
                output_filepath, f"{endpoint}/api/v1/{self.bucket}/{self.object_name(job)}", headers={}
            )  # BT: fix the headers
            return taskmodule.TemplateInfo(output_filepath, epilogue=epilogue)
        return await super().template(job, task, kind)


class YamlMetadataS3Repository(YamlMetadataRepository, S3BucketRepository):
    """A metadata repository based on a s3 bucket repository."""

    def __init__(self, client, bucket, prefix, suffix=".yaml", mimetype="text/yaml"):
        super().__init__(client, bucket, prefix, suffix=suffix, mimetype=mimetype)

    @job_getter
    async def info(self, job):
        try:
            return await super().info(job)
        except botocore.exceptions.ClientError as e:
            if "NoSuchKey" in str(e):
                return {}
            else:
                raise
