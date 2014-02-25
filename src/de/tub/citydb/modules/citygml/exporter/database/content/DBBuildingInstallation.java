/*
 * This file is part of the 3D City Database Importer/Exporter.
 * Copyright (c) 2007 - 2013
 * Institute for Geodesy and Geoinformation Science
 * Technische Universitaet Berlin, Germany
 * http://www.gis.tu-berlin.de/
 * 
 * The 3D City Database Importer/Exporter program is free software:
 * you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program. If not, see 
 * <http://www.gnu.org/licenses/>.
 * 
 * The development of the 3D City Database Importer/Exporter has 
 * been financially supported by the following cooperation partners:
 * 
 * Business Location Center, Berlin <http://www.businesslocationcenter.de/>
 * virtualcitySYSTEMS GmbH, Berlin <http://www.virtualcitysystems.de/>
 * Berlin Senate of Business, Technology and Women <http://www.berlin.de/sen/wtf/>
 */
package de.tub.citydb.modules.citygml.exporter.database.content;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.citygml4j.model.citygml.CityGMLClass;
import org.citygml4j.model.citygml.building.AbstractBuilding;
import org.citygml4j.model.citygml.building.BuildingInstallation;
import org.citygml4j.model.citygml.building.BuildingInstallationProperty;
import org.citygml4j.model.citygml.building.IntBuildingInstallation;
import org.citygml4j.model.citygml.building.IntBuildingInstallationProperty;
import org.citygml4j.model.citygml.building.Room;
import org.citygml4j.model.citygml.core.ImplicitGeometry;
import org.citygml4j.model.citygml.core.ImplicitRepresentationProperty;
import org.citygml4j.model.gml.basicTypes.Code;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryProperty;

import de.tub.citydb.api.geometry.GeometryObject;
import de.tub.citydb.config.Config;
import de.tub.citydb.util.Util;

public class DBBuildingInstallation implements DBExporter {
	private final DBExporterManager dbExporterManager;
	private final Config config;
	private final Connection connection;

	private PreparedStatement psBuildingInstallation;
	private PreparedStatement psRoomInstallation;

	private DBSurfaceGeometry surfaceGeometryExporter;
	private DBCityObject cityObjectReader;
	private DBThematicSurface thematicSurfaceExporter;
	private DBImplicitGeometry implicitGeometryExporter;
	private DBOtherGeometry geometryExporter;

	public DBBuildingInstallation(Connection connection, Config config, DBExporterManager dbExporterManager) throws SQLException {
		this.connection = connection;
		this.config = config;
		this.dbExporterManager = dbExporterManager;

		init();
	}

	private void init() throws SQLException {
		if (!config.getInternal().isTransformCoordinates()) {
			StringBuilder query = new StringBuilder()
			.append("select ID, OBJECTCLASS_ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, ")
			.append("LOD2_BREP_ID, LOD3_BREP_ID, LOD4_BREP_ID, ")
			.append("LOD2_OTHER_GEOM, LOD3_OTHER_GEOM, LOD4_OTHER_GEOM, ")
			.append("LOD2_IMPLICIT_REP_ID, LOD3_IMPLICIT_REP_ID, LOD4_IMPLICIT_REP_ID, ")
			.append("LOD2_IMPLICIT_REF_POINT, LOD3_IMPLICIT_REF_POINT, LOD4_IMPLICIT_REF_POINT, ")
			.append("LOD2_IMPLICIT_TRANSFORMATION, LOD3_IMPLICIT_TRANSFORMATION, LOD4_IMPLICIT_TRANSFORMATION ")
			.append("from BUILDING_INSTALLATION where ");

			psBuildingInstallation = connection.prepareStatement(query.toString() + "BUILDING_ID = ?");
			psRoomInstallation = connection.prepareStatement(query.toString() + "ROOM_ID =  ?");
		} else {
			int srid = config.getInternal().getExportTargetSRS().getSrid();
			String transformOrNull = dbExporterManager.getDatabaseAdapter().getSQLAdapter().resolveDatabaseOperationName("geodb_util.transform_or_null");

			StringBuilder query = new StringBuilder()
			.append("select ID, OBJECTCLASS_ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, ")
			.append("LOD2_BREP_ID, LOD3_BREP_ID, LOD4_BREP_ID, ")
			.append(transformOrNull).append("(LOD2_OTHER_GEOM, ").append(srid).append(") AS LOD2_OTHER_GEOM, ")
			.append(transformOrNull).append("(LOD3_OTHER_GEOM, ").append(srid).append(") AS LOD3_OTHER_GEOM, ")
			.append(transformOrNull).append("(LOD4_OTHER_GEOM, ").append(srid).append(") AS LOD4_OTHER_GEOM, ")
			.append("LOD2_IMPLICIT_REP_ID, LOD3_IMPLICIT_REP_ID, LOD4_IMPLICIT_REP_ID, ")
			.append(transformOrNull).append("(LOD2_IMPLICIT_REF_POINT, ").append(srid).append(") AS LOD2_IMPLICIT_REF_POINT, ")
			.append(transformOrNull).append("(LOD3_IMPLICIT_REF_POINT, ").append(srid).append(") AS LOD3_IMPLICIT_REF_POINT, ")
			.append(transformOrNull).append("(LOD4_IMPLICIT_REF_POINT, ").append(srid).append(") AS LOD4_IMPLICIT_REF_POINT, ")
			.append("LOD2_IMPLICIT_TRANSFORMATION, LOD3_IMPLICIT_TRANSFORMATION, LOD4_IMPLICIT_TRANSFORMATION ")
			.append("from BUILDING_INSTALLATION where ");

			psBuildingInstallation = connection.prepareStatement(query.toString() + "BUILDING_ID = ?");
			psRoomInstallation = connection.prepareStatement(query.toString() + "ROOM_ID =  ?");
		}

		surfaceGeometryExporter = (DBSurfaceGeometry)dbExporterManager.getDBExporter(DBExporterEnum.SURFACE_GEOMETRY);
		cityObjectReader = (DBCityObject)dbExporterManager.getDBExporter(DBExporterEnum.CITYOBJECT);
		thematicSurfaceExporter = (DBThematicSurface)dbExporterManager.getDBExporter(DBExporterEnum.THEMATIC_SURFACE);
		implicitGeometryExporter = (DBImplicitGeometry)dbExporterManager.getDBExporter(DBExporterEnum.IMPLICIT_GEOMETRY);
		geometryExporter = (DBOtherGeometry)dbExporterManager.getDBExporter(DBExporterEnum.OTHER_GEOMETRY);
	}

	public void read(AbstractBuilding building, long parentId) throws SQLException {
		ResultSet rs = null;

		try {
			psBuildingInstallation.setLong(1, parentId);
			rs = psBuildingInstallation.executeQuery();

			while (rs.next()) {
				long installationId = rs.getLong(1);

				int classId = rs.getInt(2);
				if (rs.wasNull() || classId == 0)
					continue;

				BuildingInstallation buildingInstallation = null;
				IntBuildingInstallation intBuildingInstallation = null;

				CityGMLClass type = Util.classId2cityObject(classId);
				switch (type) {
				case BUILDING_INSTALLATION:
					buildingInstallation = new BuildingInstallation();
					break;
				case INT_BUILDING_INSTALLATION:
					intBuildingInstallation = new IntBuildingInstallation();
					break;
				default:
					continue;
				}

				String clazz = rs.getString(3);
				if (clazz != null) {
					Code code = new Code(clazz);
					code.setCodeSpace(rs.getString(4));
					if (buildingInstallation != null)
						buildingInstallation.setClazz(code);
					else
						intBuildingInstallation.setClazz(code);
				}

				String function = rs.getString(5);
				String functionCodeSpace = rs.getString(6);
				if (function != null) {
					if (buildingInstallation != null)
						buildingInstallation.setFunction(Util.string2codeList(function, functionCodeSpace));
					else
						intBuildingInstallation.setFunction(Util.string2codeList(function, functionCodeSpace));
				}

				String usage = rs.getString(7);
				String usageCodeSpace = rs.getString(8);
				if (usage != null) {
					if (buildingInstallation != null)
						buildingInstallation.setUsage(Util.string2codeList(usage, usageCodeSpace));
					else
						intBuildingInstallation.setUsage(Util.string2codeList(usage, usageCodeSpace));
				}

				// boundarySurface
				// geometry objects of _BoundarySurface elements have to be referenced by lodXGeometry 
				// So we first export all _BoundarySurfaces
				if (buildingInstallation != null)
					thematicSurfaceExporter.read(buildingInstallation, installationId);
				else
					thematicSurfaceExporter.read(intBuildingInstallation, installationId);					

				// geometry
				for (int lod = 0; lod < 3; lod++) {
					long surfaceGeometryId = rs.getLong(9 + lod);
					Object geomObj = rs.getObject(12 + lod);
					if (surfaceGeometryId == 0 && geomObj == null)
						continue;

					GeometryProperty<AbstractGeometry> geometryProperty = null;

					if (surfaceGeometryId != 0) {
						DBSurfaceGeometryResult geometry = surfaceGeometryExporter.read(surfaceGeometryId);
						if (geometry != null) {
							geometryProperty = new GeometryProperty<AbstractGeometry>();
							if (geometry.getAbstractGeometry() != null)
								geometryProperty.setGeometry(geometry.getAbstractGeometry());
							else
								geometryProperty.setHref(geometry.getTarget());
						}
					} else {
						GeometryObject geometry = dbExporterManager.getDatabaseAdapter().getGeometryConverter().getGeometry(geomObj);
						if (geometry != null) {
							geometryProperty = new GeometryProperty<AbstractGeometry>();
							geometryProperty.setGeometry(geometryExporter.getPointOrCurveGeometry(geometry, true));
						}	
					}

					if (geometryProperty != null) {
						switch (lod) {
						case 0:
							if (buildingInstallation != null)
								buildingInstallation.setLod2Geometry(geometryProperty);
							break;
						case 1:
							if (buildingInstallation != null)
								buildingInstallation.setLod3Geometry(geometryProperty);
							break;
						case 2:
							if (buildingInstallation != null)
								buildingInstallation.setLod4Geometry(geometryProperty);
							else
								intBuildingInstallation.setLod4Geometry(geometryProperty);
							break;
						}
					}
				}

				// implicit geometry
				for (int lod = 0; lod < 3; lod++) {
					long implicitGeometryId = rs.getLong(15 + lod);
					if (rs.wasNull())
						continue;

					GeometryObject referencePoint = null;
					Object referencePointObj = rs.getObject(18 + lod);
					if (!rs.wasNull() && referencePointObj != null)
						referencePoint = dbExporterManager.getDatabaseAdapter().getGeometryConverter().getPoint(referencePointObj);

					String transformationMatrix = rs.getString(21 + lod);

					ImplicitGeometry implicit = implicitGeometryExporter.read(implicitGeometryId, referencePoint, transformationMatrix);
					if (implicit != null) {
						ImplicitRepresentationProperty implicitProperty = new ImplicitRepresentationProperty();
						implicitProperty.setObject(implicit);

						switch (lod) {
						case 0:
							if (buildingInstallation != null)
								buildingInstallation.setLod3ImplicitRepresentation(implicitProperty);
							break;
						case 1:
							if (buildingInstallation != null)
								buildingInstallation.setLod4ImplicitRepresentation(implicitProperty);
							else
								intBuildingInstallation.setLod4ImplicitRepresentation(implicitProperty);
							break;
						}
					}
				}

				if (buildingInstallation != null) {
					cityObjectReader.read(buildingInstallation, installationId);

					BuildingInstallationProperty buildInstProp = new BuildingInstallationProperty();
					buildInstProp.setObject(buildingInstallation);
					building.addOuterBuildingInstallation(buildInstProp);
				} else {
					cityObjectReader.read(intBuildingInstallation, installationId);

					IntBuildingInstallationProperty intInstProp = new IntBuildingInstallationProperty();
					intInstProp.setObject(intBuildingInstallation);
					building.addInteriorBuildingInstallation(intInstProp);
				}
			}
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	public void read(Room room, long parentId) throws SQLException {
		ResultSet rs = null;

		try {
			psRoomInstallation.setLong(1, parentId);
			rs = psRoomInstallation.executeQuery();

			while (rs.next()) {
				long installationId = rs.getLong(1);

				int classId = rs.getInt(2);
				if (rs.wasNull() || classId == 0 || Util.classId2cityObject(classId) != CityGMLClass.INT_BUILDING_INSTALLATION)
					continue;

				IntBuildingInstallation intBuildingInstallation = new IntBuildingInstallation();

				String clazz = rs.getString(3);
				if (clazz != null) {
					Code code = new Code(clazz);
					code.setCodeSpace(rs.getString(4));
					intBuildingInstallation.setClazz(code);
				}

				String function = rs.getString(5);
				String functionCodeSpace = rs.getString(6);
				if (function != null)
					intBuildingInstallation.setFunction(Util.string2codeList(function, functionCodeSpace));

				String usage = rs.getString(7);
				String usageCodeSpace = rs.getString(8);
				if (usage != null)
					intBuildingInstallation.setUsage(Util.string2codeList(usage, usageCodeSpace));
				
				// boundarySurface
				// geometry objects of _BoundarySurface elements have to be referenced by lod4Geometry 
				// So we first export all _BoundarySurfaces
				thematicSurfaceExporter.read(intBuildingInstallation, installationId);	

				// geometry
				long surfaceGeometryId = rs.getLong(11);
				Object geomObj = rs.getObject(14);
				if (surfaceGeometryId != 0 || geomObj != null) {
					GeometryProperty<AbstractGeometry> geometryProperty = null;
					if (surfaceGeometryId != 0) {
						DBSurfaceGeometryResult geometry = surfaceGeometryExporter.read(surfaceGeometryId);
						if (geometry != null) {
							geometryProperty = new GeometryProperty<AbstractGeometry>();
							if (geometry.getAbstractGeometry() != null)
								geometryProperty.setGeometry(geometry.getAbstractGeometry());
							else
								geometryProperty.setHref(geometry.getTarget());
						}
					} else {
						GeometryObject geometry = dbExporterManager.getDatabaseAdapter().getGeometryConverter().getGeometry(geomObj);
						if (geometry != null) {
							geometryProperty = new GeometryProperty<AbstractGeometry>();
							geometryProperty.setGeometry(geometryExporter.getPointOrCurveGeometry(geometry, true));
						}	
					}

					if (geometryProperty != null)
						intBuildingInstallation.setLod4Geometry(geometryProperty);
				}

				// implicit geometry
				long implicitGeometryId = rs.getLong(17);
				if (implicitGeometryId != 0) {
					GeometryObject referencePoint = null;
					Object referencePointObj = rs.getObject(20);
					if (!rs.wasNull() && referencePointObj != null)
						referencePoint = dbExporterManager.getDatabaseAdapter().getGeometryConverter().getPoint(referencePointObj);

					String transformationMatrix = rs.getString(23);

					ImplicitGeometry implicit = implicitGeometryExporter.read(implicitGeometryId, referencePoint, transformationMatrix);
					if (implicit != null) {
						ImplicitRepresentationProperty implicitProperty = new ImplicitRepresentationProperty();
						implicitProperty.setObject(implicit);
						intBuildingInstallation.setLod4ImplicitRepresentation(implicitProperty);
					}
				}

				cityObjectReader.read(intBuildingInstallation, installationId);

				IntBuildingInstallationProperty intInstProp = new IntBuildingInstallationProperty();
				intInstProp.setObject(intBuildingInstallation);
				room.addRoomInstallation(intInstProp);
			}
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	@Override
	public void close() throws SQLException {
		psBuildingInstallation.close();
		psRoomInstallation.close();
	}

	@Override
	public DBExporterEnum getDBExporterType() {
		return DBExporterEnum.BUILDING_INSTALLATION;
	}

}
