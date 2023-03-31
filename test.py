from pydala.dataset.reader import Reader


def main():
    r = Reader(
        "MMS/raw/val/all", bucket="dswb-nes-data/EWN", protocol="s3", profile="default"
    )
    r.logger.info(r.__class__)


main()
# from pydala.utils.logging import get_logger

# ll=get_logger("Name")
# ll.info("ABC")
