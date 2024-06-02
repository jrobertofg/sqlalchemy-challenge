from flask import Flask, jsonify
import datetime as dt
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker

#################################################
# Database Setup
#################################################

engine = create_engine(r"sqlite:///C:\Users\jrobe\Documents\Challenge 10\sqlalchemy-challenge\Resources\hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables using autoload_with
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session factory and scoped session
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available routes."""
    return (
        f"Welcome to the Climate App API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the JSON representation of precipitation data for the last 12 months."""
    session = Session()
    try:
        # Calculate the date 1 year ago from the last data point in the database
        prev_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
        
        # Query precipitation data
        results = session.query(Measurement.date, Measurement.prcp).\
                  filter(Measurement.date >= prev_year).all()
        
        # Convert the query results to a dictionary
        precipitation_data = {date: prcp for date, prcp in results}
        
        return jsonify(precipitation_data)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        session.close()

@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of stations from the dataset."""
    session = Session()
    try:
        # Query stations
        results = session.query(Station.station).all()
        
        # Convert the query results to a list
        station_list = list(np.ravel(results))
        
        return jsonify(station_list)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        session.close()

@app.route("/api/v1.0/tobs")
def tobs():
    """Return a JSON list of temperature observations for the previous year."""
    session = Session()
    try:
        # Calculate the date 1 year ago from the last data point in the database
        prev_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
        
        # Query the most active station
        most_active_station = session.query(Measurement.station).\
                              group_by(Measurement.station).\
                              order_by(func.count(Measurement.station).desc()).\
                              first()[0]
        
        # Query temperature data for the most active station
        results = session.query(Measurement.date, Measurement.tobs).\
                  filter(Measurement.station == most_active_station).\
                  filter(Measurement.date >= prev_year).all()
        
        # Convert the query results to a list of dictionaries
        temperature_data = [{"date": date, "tobs": tobs} for date, tobs in results]
        
        return jsonify(temperature_data)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        session.close()

@app.route("/api/v1.0/<start>")
def start_date(start):
    """Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start date."""
    session = Session()
    try:
        # Query temperature data for dates greater than or equal to the start date
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                  filter(Measurement.date >= start).all()
        
        # Convert the query results to a list
        temp_stats = list(np.ravel(results))
        
        return jsonify(temp_stats)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        session.close()

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    """Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start-end range."""
    session = Session()
    try:
        # Query temperature data for dates between the start and end dates
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                  filter(Measurement.date >= start).\
                  filter(Measurement.date <= end).all()
        
        # Convert the query results to a list
        temp_stats = list(np.ravel(results))
        
        return jsonify(temp_stats)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True)