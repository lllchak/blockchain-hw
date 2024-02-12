from argparse import (
    ArgumentParser,
    BooleanOptionalAction,
    Namespace,
)
import logging
from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
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

    return parser


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    arg_parser: ArgumentParser = with_arguments(ArgumentParser())
    args: Namespace = arg_parser.parse_args()

    db: TDBClient = TDBClient(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        port=args.port,
    )
    db.drop_table(table_name="users")
    db.create_table(
        table_name="users",
        user_id="serial primary key",
        username="varchar (50) unique not null",
        password="varchar (50) not null",
        email="varchar (255) unique not null",
    )
    db.insert_into(
        table_name="users",
        schema="username, password, email",
        values=[
            ("lllchak", 123, "email@email.com"),
            ("mediumchak", 456, "maile@maile.com"),
        ]
    )
    res: List[Tuple[Any]] = db.execute("select * from users;", fetch_all=True)

    app = Flask(__name__)
    blockchain = TBlockchain()

    @app.route("/mine_block", methods=["GET", "POST"])
    def mine_block():
        previous_block: TBlock = blockchain.last_block()
        proof: int = blockchain.proof_of_work(previous_block["proof"])
        previous_hash: bytes = blockchain.hash(previous_block)
        block: TBlock = blockchain.create_block(
            proof=proof,
            data=TBlockData(
                username=res[-1][1],
                password=res[-1][2],
                email=res[-1][3]
            ),
            hashed_data=previous_hash,
        )

        response: Dict[str, Any] = {
            "message": "Block is mined",
            "index": block["index"],
            "data": block["data"],
            "hashed_data": block["hashed_data"],
            "proof": block["proof"],
        }

        return jsonify(response), 200

    @app.route("/display_chain", methods=["GET", "POST"])
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
        app.run(debug = True)

    db.close()
