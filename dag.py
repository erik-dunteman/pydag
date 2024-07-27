import concurrent.futures
from graphlib import TopologicalSorter
import time, inspect
import threading

from colorama import Fore, Style, init
init(autoreset=True)

class DagRunner:
    def __init__(self):
        self.dag = {}
        self.results = {}
        self.ts = None
    
    def _node(self, func=None, var=None, returns=[], args=[]):
        if bool(func) == bool(var):
            raise ValueError("Node must be defined with either 'func' or 'var'.")
        if func:
            for res in returns:
                self.dag[res] = {"deps": set(args), "fn": func, "state": "not_ready"}
        else:
            if not returns:
                raise ValueError("Variable nodes must have 'returns' defined.")
            for res in returns:
                self.dag[res] = {"deps": set(), "var": var, "state": "not_ready"}

    def get(self, id):
        return self.results[id]
    
    def pretty_print_loop(self):
        while True:
            self.pretty_print()
            if all (value["state"] == "done" for value in self.dag.values()):
                break
            time.sleep(0.1)
    
    def pretty_print(self):
        # print full screen of whitespace to clear the screen
        print("\n" * 100)
        statuses = {}
        for node, value in self.dag.items():
            statuses[node] = value["state"]
        
        def colorize_deps(deps):
            deps = [f"{Fore.YELLOW}{dep}" if statuses[dep] == "running" else f"{Fore.GREEN}{dep}" if statuses[dep] == "done" else f"{Fore.WHITE}{dep}" for dep in deps]
            return f"[{', '.join(deps)}{Style.RESET_ALL}]"

        for node, status in statuses.items():
            if status == "not_ready":
                print(f"{Fore.WHITE}{node}\tawaiting {colorize_deps(self.dag[node]['deps'])}")
            elif status == "running":
                since = time.time() - self.dag[node]["startTime"]
                print(f"{Fore.YELLOW}{node}\trunning\t\t\t({round(since, 1)}s)")
            elif status == "done":
                print(f"{Fore.GREEN}{node}\tdone")

    def run(self):

        # thread this self.pretty_print()
        threading.Thread(target=self.pretty_print_loop).start()


        def run_node(node):
            args = [self.results[arg] for arg in self.dag[node]["deps"]]
            if "fn" in self.dag[node]:
                result = self.dag[node]["fn"](*args)
            else:
                result = self.dag[node]["var"]

            self.results[node] = result

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            while True:
                ready_nodes = list(self.ts.get_ready())
                if not ready_nodes and not futures:
                    break

                for node in ready_nodes:
                    # print(f"Submitting {node} for execution")
                    futures[node] = executor.submit(run_node, node)
                    self.dag[node]["state"] = "running"
                    self.dag[node]["startTime"] = time.time()
                    self.pretty_print()

                if futures:
                    done, _ = concurrent.futures.wait(futures.values(), return_when=concurrent.futures.FIRST_COMPLETED)
                    for future in done:
                        node = [k for k, v in futures.items() if v == future][0]
                        try:
                            future.result()
                            self.ts.done(node)
                            self.dag[node]["state"] = "done"
                            del futures[node]
                            self.pretty_print()
                        except Exception as exc:
                            print(f'{node} generated an exception: {exc}')

class Dag:
    def __enter__(self):
        self.dag_runner = DagRunner()
        self.start_frame = inspect.currentframe().f_back
        self.start_locals = self.start_frame.f_locals.copy()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_locals = self.start_frame.f_locals
        new_locals = {k: v for k, v in end_locals.items() if k not in self.start_locals or self.start_locals[k] != v}

        for key, value in new_locals.items():
            if isinstance(value, Dag):
                continue  # Skip the self reference in methods
            if inspect.isfunction(value):
                args = inspect.getfullargspec(value).args
                self.dag_runner._node(func=value, returns=[key], args=args)
            else:
                self.dag_runner._node(var=value, returns=[key])

        self.dag_runner.ts = TopologicalSorter({key: value["deps"] for key, value in self.dag_runner.dag.items()})
        self.dag_runner.ts.prepare()

    def get(self, id):
        return self.dag_runner.get(id)

    def run(self):
        self.dag_runner.run()


