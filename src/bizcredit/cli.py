
import json, sys
from . import pipeline
from . import framework

def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if "--status" in argv:
        # Read-only: report durable verification state without touching the portal.
        print(json.dumps(framework.submission_status(), ensure_ascii=False))
        return
    mode = "full"
    if "--incremental" in argv:
        mode = "incremental"
    if "--full" in argv:
        mode = "full"
    print(json.dumps(pipeline.run(mode=mode), ensure_ascii=False))

if __name__ == "__main__":
    main()
