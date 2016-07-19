from gather_scatter import Gatherer
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Starts the Gatherer service.')
    parser.add_argument('agents', metavar='A', type=str, nargs='+',
                    help='Agents to specifically wait for ready signal')
    args = parser.parse_args()

    gatherer = Gatherer(args.agents)
    print("Gatherer will wait for the following agents:")
    print(", ".join(args.agents))
    print("Starting gatherer")
    gatherer.start()
