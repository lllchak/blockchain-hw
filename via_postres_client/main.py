import random
import string
from argparse import (
    ArgumentParser,
    BooleanOptionalAction,
    Namespace,
)
import logging
import json
from typing import (
    Any,
    Dict,
    List,
    Tuple,
)

from flask import (
    Flask,
    jsonify,
)

from db_client import TDBClient
from chain import (
    TBlockchain,
    TBlock,
    TBlockData,
)


def with_arguments(parser: ArgumentParser) -> ArgumentParser:
    parser.add_argument(
        "-n", "--dbname",
        type=str,
        default="postgres",
        help="DB name",
    )
    parser.add_argument(
        "-u", "--user",
        type=str,
        default="unknown",
        help="DB user",
    )
    parser.add_argument(
        "-c", "--password",
        type=str,
        default="",
        help="DB password",
    )
    parser.add_argument(
        "-p", "--port",
        type=str,
        default="5000",
        help="DB port",
    )
    parser.add_argument(
        "-r", "--run-app",
        action=BooleanOptionalAction,
        help="Run app flag",
    )
    parser.add_argument(
        "-t", "--prepare-table",
        action=BooleanOptionalAction,
        help="Prepare table flag"
    )

    return parser


def generate_data(n_samples: int = 100) -> List[Tuple[Any]]:
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'William']
    last_names = ['Doe', 'Smith', 'Johnson', 'Brown', 'Davis']
    additional_first_names = ['Anna', 'David', 'Sarah', 'Brian', 'Olivia']
    additional_last_names = ['Martinez', 'Anderson', 'Wilson', 'Garcia', 'Taylor']
    first_names.extend(additional_first_names)
    last_names.extend(additional_last_names)

    def random_string(length):
        return ''.join(random.choices(string.ascii_letters, k=length))

    return [
        (
            random_string(15),
            random.choice([ True, False]),
            random_string(7),
            f"{random_string(8)}@example.com",
            ''.join(random.choices(string.ascii_uppercase + string.digits, k=7)),
            random.uniform(1000.0, 10000.0),
            False,
        )
        for _ in range(n_samples)
    ]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    arg_parser: ArgumentParser = with_arguments(ArgumentParser())
    args: Namespace = arg_parser.parse_args()
    table_name: str = "caas_users"

    db: TDBClient = TDBClient(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        port=args.port,
    )
    if args.prepare_table:
        db.drop_table("caas_users")
        db.create_table(
            table_name=table_name,
            user_id="serial primary key",
            username="varchar (50) unique not null",
            sex="boolean not null",
            password="varchar (50) not null",
            email="varchar (255) unique not null",
            licence_id="varchar(255) unique not null",
            bank_account="real not null",
            mined="boolean not null",
        )
        db.insert_into(
            table_name=table_name,
            schema="username, sex, password, email, licence_id, bank_account, mined",
            values=generate_data()
        )

    app = Flask(__name__)
    blockchain = TBlockchain()

    @app.route("/mine_block", methods=["GET"])
    @app.route("/mine_block/<block_index>", methods=["GET"])
    def mine_block(block_index: int = -1):
        res: List[Tuple[Any]] = (
            db.execute(f"select * from {table_name};", fetch_all=True)
            if block_index == -1
            else
            db.execute(f"select * from {table_name} where user_id = {block_index};", fetch_all=True)
        )

        resp_blocks: List[Dict[Any]] = []

        for row in res:
            logging.info(f"Mining block {row[0]}")
            previous_block: TBlock = blockchain.last_block()
            proof: int = blockchain.proof_of_work(previous_block["proof"])
            previous_hash: bytes = blockchain.hash(previous_block)
            block: TBlock = blockchain.create_block(
                proof=proof,
                data=TBlockData(
                    username=row[1],
                    sex=row[2],
                    password=row[3],
                    email=row[4],
                    licence_id=row[5],
                    bank_account=row[6],
                    mined=True,
                ),
                hashed_data=previous_hash,
            )
            resp_blocks.append(json.loads(json.dumps(block)))

        response: Dict[str, Any] = {
            "message": f"{len(res)} block(-s) mined",
            "blocks": resp_blocks,
        }

        return jsonify(response), 200

    @app.route("/display_chain", methods=["GET"])
    def display_chain():
        response: Dict[str, Any] = {
            "chain": blockchain.chain,
            "length": len(blockchain.chain),
        }
        return jsonify(response), 200

    @app.route("/valid", methods=["GET", "POST"])
    def valid():
        response: Dict[str, str] = (
            {"message": "Blockchain is valid"} if blockchain.is_valid() else
            {"message": "Blockchain is invalid"}
        )
        return jsonify(response), 200

    if args.run_app:
        app.run(debug=True)

    db.close()
