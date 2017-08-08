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
	 * from X2 Development Corporation.
	 *
	 * ====================================================================
	 */

	import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;

import java.io.ByteArrayInputStream;
import java.io.InputStream; 
import java.util.Collection; 
import java.util.Map;

import org.apache.ojb.broker.query.Criteria;
import org.apache.ojb.broker.query.QueryByCriteria;

import com.follett.fsc.core.k12.beans.ExtendedDataDictionary;
import com.follett.fsc.core.k12.beans.FormInstance;
import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.Report;
import com.follett.fsc.core.k12.beans.WorkflowProgressForm;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;

import net.sf.jasperreports.engine.JRDataSource;

	import com.x2dev.sis.model.beans.*;
import com.x2dev.utils.types.PlainDate;
import com.follett.fsc.core.k12.tools.reports.*;
import com.follett.fsc.core.k12.web.*; 
import com.google.gdata.util.common.base.StringUtil;
import com.lowagie.text.pdf.hyphenation.TernaryTree.Iterator;

	/**
	 * Prepares the data for the Daily Attendance Post report. This report lists all the staff, ordered
	 * by school, and what time (if ever) they posted daily attendance.
	 *
	 * @author X2 Development Corporation
	 */
	public class AttendanceInterventionReport1 extends ReportJavaSourceNet
	{
	    /**
	     * 
	     */
	    private static final long serialVersionUID = 1L;

	 
	   /**
	     * Report parameter name for the attendance date. This value is a PlainDate object.
	     */
	    public static final String END_DATE_PARAM = "endDate";
	    public static final String START_DATE_PARAM = "startDate";
	   
	    /**
	     * Report column name which contains the school which the record is
	     * associated with
	     */
	    public static final String FIELD_SCHOOL = "school";
	    

	    /**
	     * Report column name which contains the Staff bean.
	     */
	    public static final String FIELD_STAFF = "staff";
	    
	    /**
	     * Report column name which contains the Staff bean.
	     */
	    
	    public static final String FIELD_STUDENT = "student";

	    /**
	     * Report column which contains the attendance dates
	     */
	    public static final String FIELD_DATE = "date";
	    
	    /**
	     * Report column which contains time stamps
	     */
	    public static final String FIELD_TIMESTAMP = "timestamp";
	      
	     	    
	    /**
	     * Report column which contains first contact
	     */
	    public static final String FIELD_CONTACT1 = "contact1";

	    /**
	     * Report column which contains first contact
	     */
	    public static final String FIELD_CONTACT2 = "contact2";
	    
	    /**
	     * Report column which contains first contact
	     */
	    public static final String FIELD_CONTACT3 = "contact3";
 	    
	    /**
	     * Report column grid of absences
	     */
	    public static final String FIELD_ABSENCES = "absences";
	    
	    /**
	     * Report column individual absence
	     */
	    public static final String FIELD_ABSENCE = "absence";
	    
 	    
	    /**
	     * Report column which map of Tardies
	     */
	    public static final String FIELD_TARDIES = "tardies";    
	  	     
	    public static final String FIVE_DAY_PLAN = "5 day plan";
	    /**
	     * Parameter used to specify whether to use ascending or descending sort
	     */
	    public static final String SORT_ORDER_INPUT        = "sortOrder";

	    /**
	     * The student Edplan object
	     */
	    public static final String ED_PLAN        = "edPlan";
	    
	    /**
	     * Student Ed Plan Meetings grid for the sub report.
	     */
	    public static final String ED_PLAN_MEETINGS        = "edPlanMeetings";
	
	    /**
	     * Student ed plan meeting object for the sub report.
	     */
	    public static final String MEETING        = "meeting";
	    
	    /**
	     * Parameter used to specify the start date for the date range used to 
	     * narrow which attendance posts are returned for the specific class section
	     */
	    public static final String QUERYBY        = "queryBy";  
	    
	    private static final String ABS_SUB_REPORT             = "AbsencesSub"; 
	    private static final String MEETING_SUB_REPORT             = "meetingSub"; 
	    public static final String EDPLAN_NAME = "Attendance Intervention Plan";
	    
	    /**
	     * Name of the Attendance officer assigned to this Ed Plan
	     */
	    private static final String OFFICER_NAME		=	"officerName";
	    /**
	     * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceDori#gatherData()
	     */
 
	    private SisStudent m_currentStudent;
	    private StudentEdPlan m_edPlan; 
	    private PlainDate districtStartDate;
	    private InputStream SUB_AB ;  
	    private InputStream SUB_MEET ;  
	    private String Status1;
	    private String Status2;
	    private String Status3;
	    
	    
	    @Override
	    protected JRDataSource gatherData()
	    { 
	    	
	      ReportDataGrid grid = new ReportDataGrid();           
	                    /*
	                     * Add the STAFF bean and Posting to a grid. C 
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
	                    
	                     grid.set(FIELD_ABSENCES, Absences()); 
	                    grid.set(ABS_SUB_REPORT, SUB_AB);
	                    grid.set(MEETING_SUB_REPORT, SUB_MEET); 
	                    if (m_edPlan.getStaff() !=null){
	                    grid.set(OFFICER_NAME, m_edPlan.getStaff().getNameView());
	                    }
	                     
	        grid.beforeTop();
	        
	        return grid;
	    }
	    
 
	protected ReportDataGrid getMeetings()    
	{
		ReportDataGrid grid = new ReportDataGrid();       
		
		Collection <StudentEdPlanMeeting>  meetings = m_edPlan.getStudentEdPlanMeetings(); 
		
		if (meetings.iterator().hasNext())
		{
			grid.append (); 
			StudentEdPlanMeeting meet = meetings.iterator().next() ;
			grid.set (MEETING, meet); 
			 
		}
		 grid.beforeTop();
		 
		return grid;
	}


	protected ReportDataGrid Absences()
	{
		PlainDate StartDate = null; 
		
		
		Criteria EdCriteria = new Criteria(); 
    	EdCriteria.addNotEqualTo(StudentEdPlan.COL_STATUS_CODE, 4);
    	EdCriteria.addNotEqualTo(StudentEdPlan.COL_STATUS_CODE, 5);
    	EdCriteria.addNotEqualTo(StudentEdPlan.COL_OID, m_edPlan.getOid());
    	EdCriteria.addEqualTo(StudentEdPlan.COL_STUDENT_OID ,  m_currentStudent.getOid());
    	EdCriteria.addEqualTo(StudentEdPlan.REL_EXTENDED_DATA_DICTIONARY + PATH_DELIMITER + ExtendedDataDictionary.COL_NAME  , EDPLAN_NAME);
    	EdCriteria.addEqualTo(StudentEdPlan.COL_FIELD_B001,   FIVE_DAY_PLAN );
        QueryByCriteria query = new QueryByCriteria(StudentEdPlan.class, EdCriteria);
        query.addOrderByDescending(StudentEdPlan.COL_EFFECTIVE_DATE );
        QueryIterator activeEdPlans = getBroker().getIteratorByQuery(query) ;
        	
        districtStartDate = this.getOrganization().getCurrentContext().getStartDate();
        
        if (activeEdPlans.hasNext())
        {
        	StudentEdPlan edPlan = (StudentEdPlan) activeEdPlans.next();
        	StartDate = edPlan.getEffectiveDate();
        	Status1 = "Start Date " + StartDate.toString();
        }
        else
        {
        	StartDate = districtStartDate;
        	Status1 = "Using District Start Date " + StartDate.toString();
        }
        
        Criteria absentCriteria = new Criteria(); 
        absentCriteria.addEqualTo(StudentAttendance.COL_STUDENT_OID ,  m_currentStudent.getOid());
        absentCriteria.addEqualTo(StudentAttendance.COL_ABSENT_INDICATOR ,"Y");
        absentCriteria.addEqualTo(StudentAttendance.COL_EXCUSED_INDICATOR ,"N");
        absentCriteria.addGreaterOrEqualThan(StudentAttendance.COL_DATE , StartDate); 
        absentCriteria.addLessOrEqualThan(StudentAttendance.COL_DATE , m_edPlan.getEffectiveDate()); 
        QueryByCriteria absentQuery = new QueryByCriteria(StudentAttendance.class, absentCriteria);
        absentQuery.addOrderByAscending(StudentAttendance.COL_DATE);
        absentQuery.setEndAtIndex(5);
        QueryIterator absenceCollection = getBroker().getIteratorByQuery(absentQuery); 

    	ReportDataGrid grid = new ReportDataGrid();   
    	
    	while (absenceCollection.hasNext())
		{ 
    		StudentAttendance absent = (StudentAttendance) absenceCollection.next();
    		Status2 ="Values Found";
			grid.append (); 
			grid.set (FIELD_ABSENCE, absent );
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
         * If we're in the context of a single student, print the report for just that student
         */
        m_currentStudent = (SisStudent) userData.getCurrentRecord(SisStudent.class); 
        m_edPlan = (StudentEdPlan) userData.getCurrentRecord(StudentEdPlan.class);
    }
    
    /**
     * @see com.x2dev.sis.tools.ToolJavaSource#initialize()
     */
    @Override
    protected void initialize()
    { 
    	Report SUB = ReportUtils.getReport("RP-AIR-01-SUB0", getBroker()); 
    	SUB_AB=  new ByteArrayInputStream(SUB.getCompiledFormat());    	
		Report SUB0 = ReportUtils.getReport("RP-AIR-01-SUB1", getBroker()); 
    	SUB_MEET=  new ByteArrayInputStream(SUB0.getCompiledFormat());
    }
    
	}