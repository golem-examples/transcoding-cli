from yapapi import Golem, Task, WorkContext
from yapapi.log import enable_default_logger
from yapapi.payload import vm
from datetime import timedelta


from ffmpeg_tools.codecs import VideoCodec
from ffmpeg_tools.formats import Container

import asyncio
import sys
import os


class TranscodingParams:
    def __init__(self):
        self.format: str = "mp4"
        self.codec: str = "h265"
        self.input: str = ""
        self.resolution = [1920, 1080]


async def main():
    # Path to file to transcode
    video_file: str = sys.argv[1]
    codec: VideoCodec = VideoCodec.from_name(sys.argv[2])

    params = TranscodingParams()
    params.codec = codec.value
    params.input = video_file

    package = await vm.repo(
        image_hash="f30fce4f0f0fcc1a613c89fa6d1edf22de121531df0b2977cb67d714",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            print('got task', task)
            format: str = task.data.format
            filename, ext = os.path.splitext(task.data.input)

            script = ctx.new_script()
            script.upload_file(task.data.input, f"/golem/resources/input_video{ext}")
            script.upload_json(
                "/golem/work/params.json",
                {
                    "command": "transcode",
                    "track": f"/golem/resources/input_video{ext}",
                    "targs": {
                        "container": task.data.format,
                        "video": {"codec": task.data.codec,},
                        "resolution": task.data.resolution,
                    },
                    "output_stream": f"/golem/output/output.{format}",
                },
            )
            script.run("/golem/scripts/run-ffmpeg.sh")
            script.download_file(f"/golem/output/output.{format}", f"output.{format}")
            yield script
            # TODO: Check if job results are valid
            # and reject by: task.reject_task(msg = 'invalid file')
            task.accept_task()

        print("Transcoding finished!")

    # TODO make this dynamic, e.g. depending on the size of files to transfer
    # worst-case time overhead for initialization, e.g. negotiation, file transfer etc.
    init_overhead: timedelta = timedelta(minutes=3)

    async with Golem(
        budget=10.0,
        subnet_tag="testnet",
    ) as engine:

        async for progress in engine.execute_tasks(worker, [Task(data=params)], payload=package, max_workers=1, timeout=init_overhead + timedelta(minutes=5)):
            print("progress=", progress)


if __name__ == "__main__":
    enable_default_logger()
    asyncio.run(main())
