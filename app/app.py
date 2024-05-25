from flask import Flask, request, jsonify
from flask_restful import Api, Resource, reqparse, fields, marshal_with
from flask_sqlalchemy import SQLAlchemy
import logging
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Boolean, Text
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import datetime
import sys

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.config['SQLALCHEMY_BINDS'] = {
    'az_west': 'postgresql://postgres:postgres@localhost:5432/AZ_west_db',
    'az_centre': 'postgresql://postgres:postgres@localhost:5432/AZ_centre_db',
    'az_est': 'postgresql://postgres:postgres@localhost:5432/AZ_est_db',
    'az_sud': 'postgresql://postgres:postgres@localhost:5432/AZ_sud_db'
}

db = SQLAlchemy(app)
api = Api(app)

engines = {key: create_engine(uri)
           for key, uri in app.config['SQLALCHEMY_BINDS'].items()}
sessions = {key: scoped_session(sessionmaker(bind=engine))
            for key, engine in engines.items()}
metadata = MetaData()


def setup_logger(name, port, log_file, level=logging.INFO):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s : \n %(message)s \n"
    )
    handler = logging.FileHandler(
        log_file, encoding="utf-8", mode="w"
    )
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    logger.propagate = False

    return logger


port = sys.argv[1] if len(sys.argv) > 1 else 'default'
logger = setup_logger("app", port, f"logs/app{port}.log")


SignalementsTable = Table('signalements', metadata,
                          Column('id', Integer, primary_key=True,
                                 autoincrement=True),
                          Column('date', Date, nullable=False),
                          Column('localization', String(150), nullable=False),
                          Column('type', String(150), nullable=False),
                          Column('additionnal_infos', Text, nullable=True),
                          Column('status', Boolean, nullable=False)
                          )

for engine in engines.values():
    metadata.create_all(engine)


signalement_resource_fields = {
    'date': fields.String,
    'localization': fields.String,
    'type': fields.String,
    'additionnal_infos': fields.String,
    'status': fields.Boolean
}


class Signalement(Resource):

    @marshal_with(signalement_resource_fields)
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('date', type=str, required=True,
                            help='Date cannot be blank')
        parser.add_argument('localization', type=str,
                            required=True, help='Localization cannot be blank')
        parser.add_argument('type', type=str, required=True,
                            help='Type cannot be blank')
        parser.add_argument('additionnal_infos', type=str)
        parser.add_argument('status', type=bool, required=True,
                            help='Status cannot be blank')
        args = parser.parse_args()

        date_obj = datetime.strptime(args['date'], '%Y-%m-%d').date()

        new_signalement = SignalementsTable.insert().values(
            date=date_obj,
            localization=args['localization'],
            type=args['type'],
            additionnal_infos=args.get('additionnal_infos'),
            status=args['status']
        )

        responses = {}

        signalement_dict = {
            'date': date_obj,
            'localization': args['localization'],
            'type': args['type'],
            'additionnal_infos': args.get('additionnal_infos'),
            'status': args['status']
        }

        for key, session in sessions.items():
            try:
                conn = session.connection()
                conn.execute(new_signalement)
                session.commit()
                logger.info(
                    f'Signalement {signalement_dict} added to {key} successfully')
                responses[key] = f'Signalement added to {key} successfully'
            except Exception as e:
                session.rollback()
                logger.error(f'Failed to add signalement to {key}: {str(e)}')
                responses[key] = f'Failed to add signalement to {key}: {str(e)}'

        return signalement_dict, 201


api.add_resource(Signalement, '/signalement')

if __name__ == '__main__':
    app.run(debug=True)
