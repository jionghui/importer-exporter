package de.tub.citydb.modules.citygml.exporter.database.content;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.GregorianCalendar;
import java.util.HashMap;

import org.citygml4j.model.citygml.core.AbstractCityObject;
import org.citygml4j.model.citygml.generics.AbstractGenericAttribute;
import org.citygml4j.model.citygml.generics.DateAttribute;
import org.citygml4j.model.citygml.generics.DoubleAttribute;
import org.citygml4j.model.citygml.generics.GenericAttributeSet;
import org.citygml4j.model.citygml.generics.IntAttribute;
import org.citygml4j.model.citygml.generics.MeasureAttribute;
import org.citygml4j.model.citygml.generics.StringAttribute;
import org.citygml4j.model.citygml.generics.UriAttribute;
import org.citygml4j.model.gml.basicTypes.Measure;

public class DBCityObjectGenericAttrib implements DBExporter {
	private final Connection connection;

	private PreparedStatement psGenericAttribute;
	private HashMap<Long, GenericAttributeSet> attributeSets;

	public DBCityObjectGenericAttrib(Connection connection) throws SQLException {
		this.connection = connection;

		init();
	}

	private void init() throws SQLException {
		StringBuilder query = new StringBuilder("select ID, PARENT_GENATTRIB_ID, ")
		.append("ATTRNAME, DATATYPE, STRVAL, INTVAL, REALVAL, URIVAL, DATEVAL, UNIT, GENATTRIBSET_CODESPACE ")
		.append("from CITYOBJECT_GENERICATTRIB where DATATYPE < 8 and CITYOBJECT_ID = ?");
		psGenericAttribute = connection.prepareStatement(query.toString());
		
		attributeSets = new HashMap<Long, GenericAttributeSet>();
	}

	public void read(AbstractCityObject cityObject, long cityObjectId) throws SQLException {
		ResultSet rs = null;

		try {
			psGenericAttribute.setLong(1, cityObjectId);
			rs = psGenericAttribute.executeQuery();

			while (rs.next()) {
				long id = rs.getLong(1);
				long parentId = rs.getLong(2);
				String attrName = rs.getString(3);
				int dataType = rs.getInt(4);

				AbstractGenericAttribute genericAttribute = null;
				GenericAttributeSet parentAttributeSet = null;

				if (parentId != 0) {
					parentAttributeSet = attributeSets.get(parentId);
					if (parentAttributeSet == null) {
						parentAttributeSet = new GenericAttributeSet();
						attributeSets.put(parentId, parentAttributeSet);
					}
				}

				switch (dataType) {
				case 1:
					String strVal = rs.getString(5);
					if (!rs.wasNull()) {
						genericAttribute = new StringAttribute();
						((StringAttribute)genericAttribute).setValue(strVal);
					}
					break;
				case 2:
					Integer intVal = rs.getInt(6);
					if (!rs.wasNull()) {
						genericAttribute = new IntAttribute();
						((IntAttribute)genericAttribute).setValue(intVal);
					}
					break;
				case 3:
					Double realVal = rs.getDouble(7);
					if (!rs.wasNull()) {							
						genericAttribute = new DoubleAttribute();
						((DoubleAttribute)genericAttribute).setValue(realVal);
					}
					break;
				case 4:
					String uriVal = rs.getString(8);
					if (!rs.wasNull()) {
						genericAttribute = new UriAttribute();
						((UriAttribute)genericAttribute).setValue(uriVal);
					}
					break;
				case 5:
					Date dateVal = rs.getDate(9);
					if (!rs.wasNull()) {
						genericAttribute = new DateAttribute();
						GregorianCalendar gregDate = new GregorianCalendar();
						gregDate.setTime(dateVal);	
						((DateAttribute)genericAttribute).setValue(gregDate);
					}
					break;
				case 6:
					Double measureVal = rs.getDouble(7);
					if (!rs.wasNull()) {
						genericAttribute = new MeasureAttribute();
						Measure measure = new Measure();
						measure.setValue(measureVal);
						measure.setUom(rs.getString(10));
						((MeasureAttribute)genericAttribute).setValue(measure);
					}
					break;
				case 7:
					genericAttribute = attributeSets.get(id);
					if (genericAttribute == null) {
						genericAttribute = new GenericAttributeSet();
						attributeSets.put(id, (GenericAttributeSet)genericAttribute);
					}

					((GenericAttributeSet)genericAttribute).setCodeSpace(rs.getString(11));
					break;
				}

				if (genericAttribute != null) {
					genericAttribute.setName(attrName);

					// assign generic attribute to city object or parent attribute set
					if (parentAttributeSet == null)
						cityObject.addGenericAttribute(genericAttribute);
					else
						parentAttributeSet.addGenericAttribute(genericAttribute);
				}
			}

			attributeSets.clear();

		} finally {
			if (rs != null)
				rs.close();
		}
	}

	@Override
	public void close() throws SQLException {
		psGenericAttribute.close();
	}

	@Override
	public DBExporterEnum getDBExporterType() {
		return DBExporterEnum.CITYOBJECT_GENERICATTRIB;
	}

}
