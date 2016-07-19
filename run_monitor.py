from gather_scatter import WorkloadMonitor
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Starts the Gatherer service.')
    parser.add_argument('--name', dest='name',
                        help='Name of the monitoring agent', default="default_agent")
    args = parser.parse_args()

    monitor = WorkloadMonitor(args.name)
    monitor.start()
    monitor.alert_monitor_ready()
    monitor.wait_for_go()

    monitor.stop()
