from gather_scatter import Gatherer
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Starts the Gatherer service.')
    parser.add_argument('agents', metavar='AGENT', type=str, nargs='*',
                    help='Agents to specifically wait for ready signal')
    args = parser.parse_args()

    gatherer = Gatherer(args.agents)
    if len(args.agents) > 0:
        print("Gatherer will wait for the following agents:")
        print(", ".join(args.agents))
    else:
        print("Gatherer is not waiting for any agents by default.")
        print("If any connect before the workload, they'll just happen to get notified.")
    print("Starting gatherer")
    gatherer.start()
