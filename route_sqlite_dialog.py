# -*- coding: utf-8 -*-
"""
/***************************************************************************
 # Route Sqlite
                                 A QGIS plugin
 This plugin creates route geometries to sqlite table containing from/to
 points using ORS
                              -------------------
        begin                : 2018-11-12
        git sha              : $Format:%H$
        copyright            : (C) 2018-19 by Mátyás Gede
        email                : saman@map.elte.hu
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import os
import requests
import qgis.utils
import json

from PyQt5 import uic
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QAction, QMessageBox, QWidget
from PyQt5.QtCore import *
from PyQt5 import QtSql
from PyQt5.QtSql import *
from collections import deque
from datetime import *
from time import *

FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'route_sqlite_dialog_base.ui'))


class RouteSqliteDialog(QtWidgets.QDialog, FORM_CLASS):
    def __init__(self, parent=None):
        """Constructor."""
        super(RouteSqliteDialog, self).__init__(parent)
        # Set up the user interface from Designer.
        # After setupUI you can access any designer object by doing
        # self.<objectname>, and you can use autoconnect slots - see
        # http://qt-project.org/doc/qt-4.8/designer-using-a-ui-file.html
        # #widgets-and-dialogs-with-auto-connect
        self.setupUi(self)
        self.fwDBFile.setFilter("SQLite files (*.sqlite)")
        # event handlers
        self.pbOpenDb.clicked.connect(self.openDb) # Open DB button
        self.cbTable.currentIndexChanged.connect(self.getFieldList) # Get field list from table
        self.pbStart.clicked.connect(self.startGcThread) # Start button
        self.pbClose.clicked.connect(self.close) # Close button
        self.pbHelp.clicked.connect(self.help) # Help button
        self.WT=None
    
    def close(self):
        """Close dialog"""
        if self.WT is not None:
            self.WT.stop()
        self.reject()
    
    def help(self):
        """Help 'dialog'"""
        QMessageBox.information(self,"Help",'Select the database file, ..., '+
            'then press "Start".<br/>Depending on the number of records, processing may take several minutes.')
        
    def openDb(self):
        """Opens DB file and refreshs table list"""
        # connect to spatialite
        dbFile=self.fwDBFile.filePath()
        con=qgis.utils.spatialite_connect(dbFile)
        cur=con.cursor()
        # select tables that have geometry column
        r=cur.execute("select distinct f_table_name from geometry_columns")
        self.cbTable.clear()
        for l in r:
            self.cbTable.addItem(l[0])
        
    def getFieldList(self):
        """Lists fields of the chosen table"""
        self.cbFromField.clear()
        self.cbToField.clear()
        # connect to spatialite
        dbFile=self.fwDBFile.filePath()
        con=qgis.utils.spatialite_connect(dbFile)
        cur=con.cursor()
        r=cur.execute("select f_geometry_column from geometry_columns where geometry_type=1 and f_table_name='"+self.cbTable.currentText()+"'")
        for f in r:
            self.cbFromField.addItem(f[0])
            self.cbToField.addItem(f[0])
    
    def startGcThread(self):
        """Starts geocoding thread"""
        
        # return to lat/lon boxes if number is not valid
        def numberError(le):
            QMessageBox.warning(self,"Error","Enter a valid number here")
            le.setFocus()
            le.selectAll()
            
        if self.pbStart.text()=="Start":
            dbFile=self.fwDBFile.filePath()
            tblName=self.cbTable.currentText()
            fromFldName=self.cbFromField.currentText()
            toFldName=self.cbToField.currentText()
            geomFld=self.leGeomField.text()
            noGeom=self.cbNoGeom.isChecked()
            # change button text to stop
            self.pbStart.setText("Stop");
            # clear log box
            self.teLog.clear()
            # create and start thread
            self.WT=WorkerThread(qgis.utils.iface.mainWindow(),dbFile,tblName,fromFldName,toFldName,geomFld,noGeom)
            self.WT.jobFinished.connect(self.jobFinishedFromThread)
            self.WT.addMsg.connect(self.msgFromThread)
            self.WT.setTotal.connect(self.setTotal)
            self.WT.setProgress.connect(self.setProgress)
            self.WT.start()
        else:
            # change button text to start
            self.pbStart.setText("Start")
            # stop working thread
            self.WT.stop()
            self.teLog.append("Process stopped")
            
    def jobFinishedFromThread( self, success ):
        if success:
            self.progressBar.setValue(self.progressBar.maximum())
        # change button text to start
        self.pbStart.setText("Start")
        # stop working thread
        self.WT.stop()

    def msgFromThread( self, msg ):
        self.teLog.append(msg)        
    
    def setTotal( self, total ):
        self.progressBar.setMaximum(total)
        
    def setProgress( self, p ):
        self.progressBar.setValue(p)

class WorkerThread( QThread ):
    # signals
    addMsg=pyqtSignal(str)
    jobFinished=pyqtSignal(bool)
    setTotal=pyqtSignal(int)
    setProgress=pyqtSignal(int)
       
    def __init__( self, parentThread,dbFile,tblName,fromFldName,toFldName,geomFld,noGeom):
        QThread.__init__( self, parentThread )
        self.dbFile=dbFile
        self.tblName=tblName
        self.fromFldName=fromFldName
        self.toFldName=toFldName
        self.geomFld=geomFld
        self.noGeom=noGeom
    def run( self ):
        self.running = True
        success = self.doWork()
        self.jobFinished.emit(success)
    def stop( self ):
        self.running = False
        pass
    def doWork( self ):
        """Starts geocoding process"""
        dbFile=self.dbFile
        tblName=self.tblName
        fromFldName=self.fromFldName
        toFldName=self.toFldName
        geomFld=self.geomFld
        
        orsKey="5b3ce3597851110001cf6248989506666387417cb96307c261b098ec"
        
        # connect to spatialite
        con=qgis.utils.spatialite_connect(dbFile)
        cur=con.cursor()
        
        # create geometry field if not exists
        r=cur.execute("SELECT * FROM "+tblName+" LIMIT 1")
        haveIt=False
        for f in cur.description:
            if (f[0]==geomFld):
                haveIt=True
                self.addMsg.emit("Geometry field '"+geomFld+"' already exists...")
                break
        if (not haveIt):
            cur.execute("select AddGeometryColumn('"+tblName+"', '"+geomFld+"', 4326, 'LINESTRING', 'XY');")
            # self.addMsg.emit("select AddGeometryColumn('"+tblName+"', '"+geomFld+"', 4326, 'LINESTRING', 'XY');")
            cur.execute("select CreateSpatialIndex('"+tblName+"', '"+geomFld+"');")
            # self.addMsg.emit("select CreateSpatialIndex('"+tblName+"', '"+geomFld+"');")
            self.addMsg.emit("Geometry field '"+geomFld+"' added.")
        # query to get distinct trips
        coordsReq="st_x("+fromFldName+")||','||st_y("+fromFldName+")||'|'||st_x("+toFldName+")||','||st_y("+toFldName+")"
        sql="select "+coordsReq+", _ROWID_ from "+tblName
        if (self.noGeom):
            sql=sql+" where "+geomFld+" is null"
        #sql=sql+" limit 20" # limit only during dev!!!
        trips=[]
        for l in cur.execute(sql):
            trips.append(l) # l[0] is the from-to point pair, l[1] is row id
        self.addMsg.emit(str(len(trips))+" trips to route...")
        # iterate over trips
        self.setTotal.emit(len(trips))
        for i in range(len(trips)):
            # emergency exit
            if (not self.running):
                self.jobFinished.emit(False)
                return False
            # send routing request
            
            url="https://api.openrouteservice.org/directions?api_key="+orsKey+"&coordinates="+trips[i][0]+"&geometry_simplify=true&profile=driving-car&geometry_format=polyline&instructions=false"
            self.addMsg.emit("Sending request for "+trips[i][0]+"...")
            # self.addMsg.emit(url)
            data=requests.get(url).json()
            # self.addMsg.emit("got answer:")
            if ("routes" in data.keys()):
                gm=data['routes'][0]['geometry']
                pl="";
                for p in gm:
                    if len(pl)>0:
                        pl=pl+","
                    pl=pl+str(p[0])+" "+str(p[1])
                # update db with geometry
                sql="update "+tblName+" set "+geomFld+"=LineStringFromText('LineString("+pl+")',4326) where _ROWID_="+str(trips[i][1])
                # self.addMsg.emit(sql);
                cur.execute(sql)
                # self.addMsg.emit("bement")
                con.commit()
            else:
                self.addMsg.emit("Error: "+data["error"]["message"]);
            self.setProgress.emit(i)
            sleep(1.5) # this is to not exceed routing service usage limits
        con.commit()
        self.addMsg.emit("I think it's ready...")  
        self.jobFinished.emit(True)
        return True
        
    def cleanUp(self):
        pass