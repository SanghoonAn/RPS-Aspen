/*
 * ====================================================================
 *
 * X2 Development Corporation
 *
 * Copyright (c) 2002-2003 X2 Development Corporation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, is not permitted without express written agreement
 * from X2 Development Corporation.
 *
 * ====================================================================
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang.time.DateUtils;
import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;

import net.sf.jasperreports.engine.JRDataSource;

import com.follett.fsc.core.k12.business.StudentManager;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.web.AppGlobals;
import com.x2dev.sis.model.beans.SisStudent;
import com.x2dev.sis.model.beans.StudentAttendance;
import com.x2dev.utils.DataGrid;
import com.x2dev.utils.types.PlainDate;
import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;

/**
 * Prepares the data for the "Organization Enrollment" report. This report shows the totals for active
 * students across the district with subtotals for schools and YOG.
 * 
 * @author X2 Development Corporation
 */
public class DistrictEnrollmentData extends ReportJavaSourceNet
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private Set<String> m_allowedGradeLevels = null;
    private Map<String, Collection<StudentAttendance>> m_absentCount;

    /**
     * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceDori#gatherData()
     */
    @Override
    protected JRDataSource gatherData()
    {
        ReportDataGrid grid = new ReportDataGrid();

        try
        {
            StringBuilder queryString = new StringBuilder(1500);

            queryString.append("SELECT SKL_SCHOOL_ID,SKL_OID, SKL_SCHOOL_NAME, STD_GRADE_LEVEL, COUNT(STD_OID) AS GRADE_TOTAL ");
            queryString.append("  FROM STUDENT, SCHOOL ");
            queryString.append(" WHERE " + StudentManager.getActiveStudentDirectSQL(getOrganization(), "STD_ENROLLMENT_STATUS"));
            queryString.append("  AND STD_SKL_OID = SKL_OID ");
            queryString.append("  AND SKL_INACTIVE_IND = '0' ");
            
            int level = getOrganization().getOrganizationDefinition().getLevel();
            queryString.append(" AND STD_ORG_OID_");
            queryString.append(level + 1);
            queryString.append(" = '");
            queryString.append(getOrganization().getOid());
            queryString.append("'");
            
            queryString.append("GROUP BY SKL_SCHOOL_ID,SKL_OID, SKL_SCHOOL_NAME, STD_GRADE_LEVEL ");
            queryString.append("ORDER BY SKL_SCHOOL_ID,SKL_OID, SKL_SCHOOL_NAME, STD_GRADE_LEVEL ");

            Connection connection = getBroker().borrowConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryString.toString());

            grid.append(flattenData(resultSet));
            grid.beforeTop();

            resultSet.close();
            statement.close();
        }
        catch (SQLException sqle)
        {
            AppGlobals.getLog().log(Level.WARNING, sqle.getMessage(), sqle);
        }
        finally
        {
            getBroker().returnConnection();
        }

        return grid;
    }
    
    private void loadAbsences()
    {
    	
    	PlainDate Today = new PlainDate(DateUtils.truncate(new Date(), java.util.Calendar.DAY_OF_MONTH));
  
    	Criteria criteria = new Criteria();
    	criteria.addEqualTo(StudentAttendance.COL_ABSENT_INDICATOR, true);
    	criteria.addEqualTo(StudentAttendance.COL_DATE , Today); 
    	criteria.addEqualToField(StudentAttendance.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_SCHOOL_OID, StudentAttendance.COL_SCHOOL_OID); 
    	
    	QueryByCriteria query = new  QueryByCriteria(StudentAttendance.class, criteria);
    	m_absentCount= getBroker().getGroupedCollectionByQuery(query, StudentAttendance.COL_SCHOOL_OID, 200) ;
    }
    
    /**
     * @see com.follett.fsc.core.k12.tools.ToolJavaSource#initialize()
     */
    @Override
    protected void initialize()
    {
        m_allowedGradeLevels = new HashSet<String>(32);
        loadAbsences();
        m_allowedGradeLevels.add("PK");
        m_allowedGradeLevels.add("K");
        m_allowedGradeLevels.add("KF");
        m_allowedGradeLevels.add("KP");
        m_allowedGradeLevels.add("01");
        m_allowedGradeLevels.add("02");
        m_allowedGradeLevels.add("03");
        m_allowedGradeLevels.add("04");
        m_allowedGradeLevels.add("05");
        m_allowedGradeLevels.add("06");
        m_allowedGradeLevels.add("07");
        m_allowedGradeLevels.add("08");
        m_allowedGradeLevels.add("09");
        m_allowedGradeLevels.add("10");
        m_allowedGradeLevels.add("11");
        m_allowedGradeLevels.add("12");
        m_allowedGradeLevels.add("SP");
    }

    /**
     * Flatten the result set from a list of school/grade level totals to a cross-tabulation of
     * schools and grades. Before:
     * <pre>
     * 
     *      School | Grade | Total
     *      -------+-------+------
     *      HS 1   | 12    |   252
     *      HS 1   | 11    |   182
     *      HS 1   | 10    |   212
     *      HS 1   | 09    |   227
     *      MS 1   | 08    |   104
     *      MS 1   | 07    |    98
     *      MS 1   | 06    |   113
     *      MS 2   | 08    |   124
     *      MS 2   | 07    |   118
     *      MS 2   | 06    |    99
     *      ...
     * 
     * </pre>
     * After:
     * <pre>
     * 
     *      School |   12 |   11 |   10 |   09 |   08 |   07 |  06 | Total
     *      -------+------+------+------+------+------+------+-----+------
     *      HS 1   |  252 |  182 |  212 |  217 |    0 |    0 |   0 |   863
     *      MS 1   |    0 |    0 |    0 |    0 |  104 |   98 | 113 |   315
     *      MS 2   |    0 |    0 |    0 |    0 |  124 |  118 |  99 |   341
     *      ...
     * 
     * </pre>
     * 
     * @param resultSet
     * 
     * @return DataGrid
     */
    private DataGrid flattenData(ResultSet resultSet) throws SQLException
    {
        DataGrid grid = new DataGrid(32);

        String currentSchoolId = "";
        while (resultSet.next())
        {
            String schoolId = resultSet.getString("SKL_SCHOOL_ID");
            String schoolOid = resultSet.getString("SKL_OID");
            if (!currentSchoolId.equals(schoolId))
            {
                currentSchoolId = schoolId;
        
                grid.append();

                grid.set("school_id", schoolId);
                grid.set("school_name", resultSet.getString("SKL_SCHOOL_NAME"));

                // Initialize all count columns to 0
                grid.set("school_total", new Integer(0));
                grid.set("grade_other", new Integer(0));
                grid.set("Present", new Integer(0)); 
                grid.set("Absent", new Integer(0)); 
                
                for (String gradeLevel : m_allowedGradeLevels)
                {
                    grid.set("grade_" + gradeLevel, new Integer(0));
                }
            }

            String gradeColumn = null;

            String gradeLevel = resultSet.getString("STD_GRADE_LEVEL");
            if (m_allowedGradeLevels.contains(gradeLevel))
            {
                // Combine various kindergarten grade levels
                if ("KF".equals(gradeLevel) || "KP".equals(gradeLevel))
                {
                    gradeLevel = "K";
                }
                
                gradeColumn = "grade_" + gradeLevel;
            }
            else
            {
                gradeColumn = "grade_other";
            }
            
            // Increment existing totals
            int increment = Integer.parseInt(resultSet.getString("GRADE_TOTAL"));
            int gradeTotal = ((Integer) grid.get(gradeColumn)).intValue() + increment;
            int schoolTotal = ((Integer) grid.get("school_total")).intValue() + increment;
            int absences = 0;
            
            grid.set(gradeColumn, new Integer(gradeTotal));
            grid.set("school_total", new Integer(schoolTotal));
            
            if (m_absentCount.containsKey(schoolOid)) absences =  m_absentCount.get(schoolOid).size();
            	
            Integer present = schoolTotal -absences;
            grid.set("Absent",absences );
            grid.set("Present",present );
        }
        
        return grid;
    }
}