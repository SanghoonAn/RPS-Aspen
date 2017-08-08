package rps.reports;                                                                                                      
/*                                                                                                                        
 * ====================================================================                                                   
 *                                                                                                                        
 * Developed by Sang An                                                                                                   
 *                                                                                                                        
 * Copyright (c) 2014-2016 Tandem Conglomerate                                                                            
 * All rights reserved.                                                                                                   
 *                                                                                                                        
 * Redistribution and use in source and binary forms, with or without                                                     
 * modification, is not permitted without a written agreement                                                             
 * from Tandem Conglomerate.                                                                                              
 *                                                                                                                        
 * ====================================================================                                                   
 */                                                                                                                       
                                                                                                                          
import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;                                             
                                                                                                                          
import java.io.ByteArrayInputStream;                                                                                      
import java.io.InputStream;                                                                                               
import java.sql.Connection;                                                                                               
import java.sql.PreparedStatement;                                                                                        
import java.sql.ResultSet;                                                                                                
import java.sql.SQLException;                                                                                             
import java.util.Collection;                                                                                              
import java.util.Map;                                                                                                     
                                                                                                                          
import org.apache.ojb.broker.query.Criteria;                                                                              
import org.apache.ojb.broker.query.QueryByCriteria;                                                                       
                                                                                                                          
import com.follett.fsc.core.k12.beans.ExtendedDataDictionary;                                                             
import com.follett.fsc.core.k12.beans.FormInstance;                                                                       
import com.follett.fsc.core.k12.beans.QueryIterator;                                                                      
import com.follett.fsc.core.k12.beans.Report;                                                                             
import com.follett.fsc.core.k12.beans.WorkflowProgressForm;                                                               
import com.follett.fsc.core.k12.beans.X2BaseBean;                                                                         
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;                                                             
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;                                                        
import com.follett.fsc.core.k12.tools.reports.ReportUtils;                                                                
import com.follett.fsc.core.k12.web.UserDataContainer;                                                                    
                                                                                                                          
import net.sf.jasperreports.engine.JRDataSource;                                                                          
                                                                                                                          
import com.x2dev.sis.model.beans.SisStudent;                                                                              
import com.x2dev.sis.model.beans.StudentAttendance;                                                                       
import com.x2dev.sis.model.beans.StudentEdPlan;                                                                           
import com.x2dev.sis.model.beans.StudentEdPlanMeeting;                                                                    
import com.x2dev.utils.types.PlainDate;                                                                                   
                                                                                                                          
/**                                                                                                                       
 * Prepares the data for the Attendance Intervention Plan report.                                                         
 *                                                                                                                        
 * @author Tandem Conglomerate                                                                                            
 */                                                                                                                       
public class AttendanceInterventionReport1 extends ReportJavaSourceNet                                                    
{                                                                                                                         
    /**                                                                                                                   
     * Report parameter name for the attendance date. This value is a PlainDate object.                                   
     */                                                                                                                   
    public static final String END_DATE_PARAM       = "endDate";                                                          
    public static final String START_DATE_PARAM     = "startDate";                                                        
    /**                                                                                                                   
     * Report column name which contains the school which the record is                                                   
     * associated with                                                                                                    
     */                                                                                                                   
    public static final String FIELD_SCHOOL         = "school";                                                           
    /**                                                                                                                   
     * Report column name which contains the Staff bean.                                                                  
     */                                                                                                                   
    public static final String FIELD_STAFF          = "staff";                                                            
    /**                                                                                                                   
     * Report column name which contains the Staff bean.                                                                  
     */                                                                                                                   
    public static final String FIELD_STUDENT        = "student";                                                          
    /**                                                                                                                   
     * Report column which contains the attendance dates                                                                  
     */                                                                                                                   
    public static final String FIELD_DATE           = "date";                                                             
    /**                                                                                                                   
     * Report column which contains time stamps                                                                           
     */                                                                                                                   
    public static final String FIELD_TIMESTAMP      = "timestamp";                                                        
    /**                                                                                                                   
     * Report column which contains first contact                                                                         
     */                                                                                                                   
    public static final String FIELD_CONTACT1       = "contact1";                                                         
    /**                                                                                                                   
     * Report column which contains first contact                                                                         
     */                                                                                                                   
    public static final String FIELD_CONTACT2       = "contact2";                                                         
    /**                                                                                                                   
     * Report column which contains first contact                                                                         
     */                                                                                                                   
    public static final String FIELD_CONTACT3       = "contact3";                                                         
    /**                                                                                                                   
     * Report column grid of absences                                                                                     
     */                                                                                                                   
    public static final String FIELD_ABSENCES       = "absences";                                                         
    /**                                                                                                                   
     * Report column individual absence                                                                                   
     */                                                                                                                   
    public static final String FIELD_ABSENCE        = "absence";                                                          
    /**                                                                                                                   
     * Report column which map of Tardies                                                                                 
     */                                                                                                                   
    public static final String FIELD_TARDIES        = "tardies";                                                          
                                                                                                                          
    public static final String FIVE_DAY_PLAN        = "5 day plan";                                                       
    /**                                                                                                                   
     * Parameter used to specify whether to use ascending or descending sort                                              
     */                                                                                                                   
    public static final String SORT_ORDER_INPUT     = "sortOrder";                                                        
    /**                                                                                                                   
     * The student Edplan object                                                                                          
     */                                                                                                                   
    public static final String ED_PLAN              = "edPlan";                                                           
                                                                                                                          
    /**                                                                                                                   
     * Student Ed Plan Meetings grid for the sub report.                                                                  
     */                                                                                                                   
    public static final String ED_PLAN_MEETINGS     = "edPlanMeetings";                                                   
    /**                                                                                                                   
     * Student ed plan meeting object for the sub report.                                                                 
     */                                                                                                                   
    public static final String MEETING              = "meeting";                                                          
    /**                                                                                                                   
     * Parameter used to specify the start date for the date range used to                                                
     * narrow which attendance posts are returned for the specific class section                                          
     */                                                                                                                   
    public static final String QUERYBY              = "queryBy";                                                          
                                                                                                                          
    private static final String ABS_SUB_REPORT      = "AbsencesSub";                                                      
    private static final String MEETING_SUB_REPORT  = "meetingSub";                                                       
    private static final String EDPLAN_NAME         = "Attendance Intervention Plan";                                     
    /**                                                                                                                   
     * Name of the Attendance officer assigned to this Ed Plan                                                            
     */                                                                                                                   
    private static final String OFFICER_NAME		=	"officerName";                                                            
                                                                                                                          
    private SisStudent      m_currentStudent;                                                                             
    private StudentEdPlan   m_edPlan;                                                                                     
    private PlainDate       m_districtStartDate;                                                                          
    private InputStream     m_absencesSubReport;                                                                          
    private InputStream     m_meetingsSubReport;                                                                          
                                                                                                                          
    /**                                                                                                                   
     * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceDori#gatherData()                                      
     */                                                                                                                   
    @Override                                                                                                             
    protected JRDataSource gatherData()                                                                                   
    {                                                                                                                     
        ReportDataGrid grid = new ReportDataGrid();                                                                       
        /*                                                                                                                
         * Add the staff bean and posting to a grid.                                                                      
         */                                                                                                               
        grid.append();                                                                                                    
        //  grid.set(FIELD_STAFF, staff);                                                                                 
        grid.set(FIELD_SCHOOL, m_currentStudent.getSchool());                                                             
        grid.set(FIELD_STUDENT, m_currentStudent );                                                                       
        if (m_currentStudent.getContact1() != null)                                                                       
        {                                                                                                                 
            grid.set(FIELD_CONTACT1, m_currentStudent.getContact1().getPerson() );                                        
        }                                                                                                                 
        if (m_currentStudent.getContact2() != null)                                                                       
        {                                                                                                                 
            grid.set(FIELD_CONTACT2, m_currentStudent.getContact2().getPerson() );                                        
        }                                                                                                                 
        if (m_currentStudent.getContact3() != null)                                                                       
        {                                                                                                                 
            grid.set(FIELD_CONTACT3, m_currentStudent.getContact3().getPerson() );                                        
        }                                                                                                                 
        grid.set(ED_PLAN, m_edPlan);                                                                                      
        grid.set(ED_PLAN_MEETINGS, getMeetings());                                                                        
        grid.set(FIELD_ABSENCES, getAbsences());                                                                          
        grid.set(ABS_SUB_REPORT, m_absencesSubReport);                                                                    
        grid.set(MEETING_SUB_REPORT, m_meetingsSubReport);                                                                
        if (m_edPlan.getStaff() !=null){                                                                                  
            grid.set(OFFICER_NAME, m_edPlan.getStaff().getNameView());                                                    
        }                                                                                                                 
                                                                                                                          
        grid.beforeTop();                                                                                                 
                                                                                                                          
        return grid;                                                                                                      
    }                                                                                                                     
                                                                                                                          
                                                                                                                          
    protected ReportDataGrid getMeetings()                                                                                
    {                                                                                                                     
        ReportDataGrid grid = new ReportDataGrid();                                                                       
                                                                                                                          
        Collection<StudentEdPlanMeeting> meetings = m_edPlan.getStudentEdPlanMeetings();                                  
                                                                                                                          
        for (StudentEdPlanMeeting meeting : meetings)                                                                     
        {                                                                                                                 
            grid.append();                                                                                                
            grid.set(MEETING, meeting);                                                                                   
        }                                                                                                                 
        grid.beforeTop();                                                                                                 
                                                                                                                          
        return grid;                                                                                                      
    }                                                                                                                     
                                                                                                                          
    protected ReportDataGrid getAbsences()                                                                                
    {                                                                                                                     
        /*                                                                                                                
         * Find absence records with a link to the passed plan                                                            
         */                                                                                                               
        StringBuilder sql = new StringBuilder(3072);                                                                      
        sql.append("SELECT  ATT_OID                                                          ");                          
        sql.append("FROM    STUDENT_ATTENDANCE                                               ");                          
        sql.append("JOIN    STUDENT_ED_PLAN ON SEP_STD_OID = ATT_STD_OID                     ");                          
        sql.append("        AND SEP_OID = '" + m_edPlan.getOid() + "'                        ");                          
        sql.append("WHERE   ATT_OID = SEP_FIELDB_001                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_002                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_003                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_004                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_005                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_006                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_007                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_008                                         ");                          
        sql.append("OR      ATT_OID = SEP_FIELDB_009                                         ");                          
                                                                                                                          
        Connection connection = getBroker().borrowConnection();                                                           
        PreparedStatement statement = null;                                                                               
        ResultSet results = null;                                                                                         
                                                                                                                          
        ReportDataGrid grid = new ReportDataGrid();                                                                       
                                                                                                                          
        try                                                                                                               
        {                                                                                                                 
            statement = connection.prepareStatement(sql.toString());                                                      
            results = statement.executeQuery();                                                                           
            while (results.next())                                                                                        
            {                                                                                                             
                String attOid = results.getString(1);                                                                     
                                                                                                                          
                StudentAttendance absence = (StudentAttendance) getBroker().getBeanByOid(StudentAttendance.class, attOid);
                grid.append();                                                                                            
                grid.set(FIELD_ABSENCE, absence);                                                                         
            }                                                                                                             
        }                                                                                                                 
        catch (SQLException sqle) {                                                                                       
            // Do nothing                                                                                                 
        }                                                                                                                 
        finally {                                                                                                         
            try {                                                                                                         
                if (results != null) { results.close(); }                                                                 
                if (statement != null) { statement.close(); }                                                             
            }                                                                                                             
            catch (Exception e) { // Do nothing                                                                           
            }                                                                                                             
            getBroker().returnConnection();                                                                               
        }                                                                                                                 
                                                                                                                          
        grid.beforeTop();                                                                                                 
                                                                                                                          
        return grid;                                                                                                      
    }                                                                                                                     
                                                                                                                          
    /**                                                                                                                   
     * @see com.follett.fsc.core.k12.tools.ToolJavaSource#saveState(com.follett.fsc.core.k12.web.UserDataContainer)       
     */                                                                                                                   
    @Override                                                                                                             
    protected void saveState(UserDataContainer userData)                                                                  
    {                                                                                                                     
        /*                                                                                                                
         * Get the current student and edPlan beans                                                                       
         */                                                                                                               
        m_currentStudent = (SisStudent) userData.getCurrentRecord(SisStudent.class);                                      
        m_edPlan = (StudentEdPlan) userData.getCurrentRecord(StudentEdPlan.class);                                        
                                                                                                                          
        if (m_currentStudent==null && m_edPlan != null) {                                                                 
            m_currentStudent = m_edPlan.getStudent();                                                                     
        }                                                                                                                 
    }                                                                                                                     
                                                                                                                          
    /**                                                                                                                   
     * @see com.x2dev.sis.tools.ToolJavaSource#initialize()                                                               
     */                                                                                                                   
    @Override                                                                                                             
    protected void initialize()                                                                                           
    {                                                                                                                     
        Report SUB = ReportUtils.getReport("RP-AIR-01-SUB0", getBroker());                                                
        m_absencesSubReport = new ByteArrayInputStream(SUB.getCompiledFormat());    	                                     
        Report SUB0 = ReportUtils.getReport("RP-AIR-01-SUB1", getBroker());                                               
        m_meetingsSubReport = new ByteArrayInputStream(SUB0.getCompiledFormat());                                         
    }                                                                                                                     
                                                                                                                          
}                                                                                                                         