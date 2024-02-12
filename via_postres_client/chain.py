import hashlib
import json
import logging
from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
)

TBlock = Dict[str, Any]


class TBlockData(NamedTuple):
    username: str = ""
    password: str = ""
    email: str = ""


class TBlockchain:
    def __init__(self):
        self.chain: List[TBlock] = []
        self.create_block(proof=1, data=TBlockData(), hashed_data="0")

    def create_block(
        self,
        proof: int,
        data: NamedTuple,
        hashed_data: bytes,
    ) -> TBlock:
        block: Dict[str, Any] = {
            "index": len(self.chain),
            "data": data,
            "hashed_data": hashed_data,
            "proof": proof,
        }
        self.chain.append(block)
        logging.info(f"Created block with data {data}")

        return block

    def last_block(self) -> TBlock:
        return self.chain[-1]

    def proof_of_work(self, previous_proof: int):
        new_proof: int = 1
        check_proof: bool = False

        while not check_proof:
            hash_operation: bytes = hashlib.sha256(
                str(new_proof**2 - previous_proof**2).encode()
            ).hexdigest()

            if hash_operation[:5] == "00000":
                check_proof = True
            else:
                new_proof += 1

        return new_proof

    def hash(self, block: TBlock):
        encoded_block: bytes = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()

    def is_valid(self):
        previous_block: TBlock = self.chain[0]
        block_index: int = 1

        while block_index < len(self.chain):
            block: TBlock = self.chain[block_index]

            if block["hashed_data"] != self.hash(previous_block):
                return False

            previous_proof = previous_block["proof"]
            proof = block["proof"]
            hash_operation = hashlib.sha256(
                str(proof ** 2 - previous_proof ** 2).encode()
            ).hexdigest()

            if hash_operation[:5] != "00000":
                return False
            previous_block = block
            block_index += 1

        return True
