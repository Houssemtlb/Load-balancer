from flask import Flask, request
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with
import logging
import os
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

app = Flask(__name__)
api = Api(app)


def setup_logger(name, log_file, level=logging.INFO):
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


logger = setup_logger("app", "logs/app.log")

Base = declarative_base()


class Signalements(Base):
    __tablename__ = "signalements"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    localization = Column(String(150), nullable=False)
    type = Column(String(150), nullable=False)
    additionnal_infos = Column(Text, nullable=True)
    status = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"{self.date} - {self.localization} - {self.type} - {self.status}"


AZ_west_engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/AZ_west_db")
AZ_west_session = sessionmaker(bind=AZ_west_engine)()

AZ_sud_engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/AZ_sud_db")
AZ_sud_session = sessionmaker(bind=AZ_sud_engine)()

AZ_est_engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/AZ_est_db")
AZ_est_session = sessionmaker(bind=AZ_est_engine)()

AZ_centre_engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/AZ_centre_db")
AZ_centre_session = sessionmaker(bind=AZ_centre_engine)()

Base.metadata.create_all(AZ_west_engine)
Base.metadata.create_all(AZ_sud_engine)
Base.metadata.create_all(AZ_est_engine)
Base.metadata.create_all(AZ_centre_engine)

signalement_resource_fields = {
    'id': fields.Integer,
    'date': fields.String,
    'localization': fields.String,
    'type': fields.String,
    'additionnal_infos': fields.String,
    'status': fields.Boolean
}


class Signalement(Resource):
    @marshal_with(signalement_resource_fields)
    def get(self, localization):
        if localization == "west":
            try:
                return AZ_west_session.query(Signalements).all()
            except:
                try:
                    return AZ_sud_session.query(Signalements).all()
                except:
                    try:
                        return AZ_est_session.query(Signalements).all()
                    except:
                        return AZ_centre_session.query(Signalements).all()
        elif localization == "sud":
            try:
                return AZ_sud_session.query(Signalements).all()
            except:
                try:
                    return AZ_west_session.query(Signalements).all()
                except:
                    try:
                        return AZ_est_session.query(Signalements).all()
                    except:
                        return AZ_centre_session.query(Signalements).all()
        elif localization == "est":
            try:
                return AZ_est_session.query(Signalements).all()
            except:
                try:
                    return AZ_sud_session.query(Signalements).all()
                except:
                    try:
                        return AZ_west_session.query(Signalements).all()
                    except:
                        return AZ_centre_session.query(Signalements).all()
        elif localization == "centre":
            try:
                return AZ_centre_session.query(Signalements).all()
            except:
                try:
                    return AZ_sud_session.query(Signalements).all()
                except:
                    try:
                        return AZ_est_session.query(Signalements).all()
                    except:
                        return AZ_west_session.query(Signalements).all()
        else:
            return "localization not found", 404

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

        new_signalement = Signalements(
            date=date_obj,
            localization=args['localization'],
            type=args['type'],
            additionnal_infos=args.get('additionnal_infos'),
            status=args['status']
        )

        sessions = [AZ_west_session, AZ_sud_session,
                    AZ_est_session, AZ_centre_session]

        try:
            for session in sessions:
                new_signalement = Signalements(
                    date=date_obj,
                    localization=args['localization'],
                    type=args['type'],
                    additionnal_infos=args.get('additionnal_infos'),
                    status=args['status']
                )
                session.add(new_signalement)
                session.commit()
                logger.info(f"Signalement {new_signalement} inserted in {session}")
                
        except Exception as e:
            for session in sessions:
                session.rollback()
            abort(
                500, message=f"An error occurred while inserting the signalement: {str(e)}")

        return new_signalement, 201

    @marshal_with(signalement_resource_fields)
    def put(self):
        parser = reqparse.RequestParser()
        parser.add_argument('id', type=int, required=True,
                            help='Id cannot be blank')
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

        sessions = [AZ_west_session, AZ_sud_session,
                    AZ_est_session, AZ_centre_session]

        try:
            for session in sessions:
                signalement = session.query(
                    Signalements).filter_by(id=args['id']).first()
                signalement.date = date_obj
                signalement.localization = args['localization']
                signalement.type = args['type']
                signalement.additionnal_infos = args.get('additionnal_infos')
                signalement.status = args['status']
                session.commit()
        except Exception as e:
            for session in sessions:
                session.rollback()
            abort(
                500, message=f"An error occurred while updating the signalement: {str(e)}")

        return signalement, 200

    def delete(self, signalement_id):
        sessions = [AZ_west_session, AZ_sud_session,
                    AZ_est_session, AZ_centre_session]

        try:
            for session in sessions:
                signalement = session.query(Signalements).filter_by(
                    id=signalement_id).first()
                session.delete(signalement)
                session.commit()
        except Exception as e:
            for session in sessions:
                session.rollback()
            abort(
                500, message=f"An error occurred while deleting the signalement: {str(e)}")

        return '', 204


api.add_resource(Signalement, '/signalement/<string:localization>',
                 '/signalement', '/signalement/<int:signalement_id>')

if __name__ == '__main__':
    app.run(debug=True)
