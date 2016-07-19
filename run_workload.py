from gather_scatter import Workload

if __name__ == "__main__":
    workload = Workload()
    workload.start()

    workload.wait_for_go(3)
    workload.send_completed()
    workload.stop()
