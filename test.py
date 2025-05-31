from src.utils.logger import logger

def test_logger():
    logger.debug("Mensaje DEBUG - Solo visible en archivo")
    logger.info("Mensaje INFO - Visible en consola y archivo")
    logger.warning("Mensaje WARNING - Visible en consola y archivo")
    logger.error("Mensaje ERROR - Visible en consola y archivo")
    try:
        1/0
    except Exception as e:
        logger.error("Error con traceback", exc_info=True)

if __name__ == "__main__":
    test_logger()