"""Tests for TCP rendezvous and unique ID exchange in launch.py."""

import multiprocessing as mp
import socket
import time
from typing import List, Union

from gradsync.launch import exchange_unique_id


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _worker(
    rank: int,
    world_size: int,
    master_addr: str,
    master_port: int,
    unique_id: Union[bytes, List[int], None],
    queue: mp.Queue,
    timeout_s: float = 10.0,
) -> None:
    try:
        res = exchange_unique_id(
            unique_id=unique_id,
            rank=rank,
            world_size=world_size,
            master_addr=master_addr,
            master_port=master_port,
            timeout_s=timeout_s,
        )
        queue.put((rank, res, None))
    except Exception as e:
        queue.put((rank, None, e))


def _run_exchange(fake_id: Union[bytes, List[int]], timeout_s: float = 10.0):
    port = _find_free_port()
    queue = mp.Queue()

    p0 = mp.Process(
        target=_worker,
        args=(0, 2, "127.0.0.1", port, fake_id, queue, timeout_s),
    )
    p1 = mp.Process(
        target=_worker,
        args=(1, 2, "127.0.0.1", port, None, queue, timeout_s),
    )

    p0.start()
    # Brief pause so rank 0 can bind/listen before rank 1 connects
    time.sleep(0.05)
    p1.start()

    p0.join(timeout=timeout_s)
    p1.join(timeout=timeout_s)

    if p0.is_alive():
        p0.terminate()
        p0.join()
    if p1.is_alive():
        p1.terminate()
        p1.join()

    assert p0.exitcode == 0, f"Rank 0 process exited with code {p0.exitcode}"
    assert p1.exitcode == 0, f"Rank 1 process exited with code {p1.exitcode}"

    results = {}
    while not queue.empty():
        rank, res, err = queue.get_nowait()
        results[rank] = (res, err)

    assert 0 in results, "Rank 0 did not return a result"
    assert 1 in results, "Rank 1 did not return a result"

    res0, err0 = results[0]
    res1, err1 = results[1]

    assert err0 is None, f"Rank 0 encountered an error: {err0}"
    assert err1 is None, f"Rank 1 encountered an error: {err1}"

    expected_bytes = bytes(fake_id) if isinstance(fake_id, list) else fake_id

    assert isinstance(res0, bytes), f"Rank 0 returned {type(res0)}, expected bytes"
    assert isinstance(res1, bytes), f"Rank 1 returned {type(res1)}, expected bytes"
    assert len(res0) == 128, f"Rank 0 ID length is {len(res0)}, expected 128"
    assert len(res1) == 128, f"Rank 1 ID length is {len(res1)}, expected 128"
    assert res0 == expected_bytes, "Rank 0 ID does not match expected bytes"
    assert res1 == expected_bytes, "Rank 1 ID does not match expected bytes"
    assert res0 == res1, "Rank 0 and Rank 1 IDs do not match"


def test_exchange_unique_id_two_processes():
    """Verify rendezvous exchange succeeds between 2 processes when ID is a list of ints."""
    fake_id = [i % 256 for i in range(128)]
    _run_exchange(fake_id)


def test_exchange_unique_id_bytes_two_processes():
    """Verify rendezvous exchange succeeds between 2 processes when ID is bytes."""
    fake_id = bytes([i % 256 for i in range(128)])
    _run_exchange(fake_id)


if __name__ == "__main__":
    test_exchange_unique_id_two_processes()
    test_exchange_unique_id_bytes_two_processes()
    print("All rendezvous tests passed successfully.")
