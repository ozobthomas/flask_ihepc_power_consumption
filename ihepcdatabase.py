from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

SQLALCHEMY_DATABASE_URI = "sqlite:////Users/nolanthomas/flask/measurabl/database/ihepc.db"
engine = create_engine("sqlite:////Users/nolanthomas/flask/measurabl/database/ihepc.db", convert_unicode=True)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

def init_db():
    import ihepcmodels
    # this should only create objects that don't already exist
    Base.metadata.create_all(bind=engine)
