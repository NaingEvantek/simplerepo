ALTER Procedure [dbo].[Call_Next_Queue_Number]
    @Branch_Info_ID INT,
    @Counter_Info_ID INT,
    @User_Info_ID INT,
	@Is_Appointment bit
AS
BEGIN
    SET NOCOUNT ON;
	
    DECLARE @QueueID INT
    DECLARE @ServiceInfoID INT
    DECLARE @Generated_Timestamp DATETIME
    DECLARE @Called_Timestamp DATETIME
    DECLARE @NextQueueNumber VARCHAR(4)
    DECLARE @ServingQueueID INT
    DECLARE @Ended_Timestamp DATETIME
	DECLARE @Priority INT
	DECLARE @SortingKey INT
	--- FPB ------
    Declare @Is_FPBBranch bit
	Declare @IS_FPBCounter bit
	Declare @Senior_Ratio int
	Declare @NonSenior_Ratio int
	Declare @Senior_Ratio_Count int
	Declare @NonSenior_Ratio_Count int
	Declare @SeniorStartTime time(7)
	Declare @SeniorEndTime time(7)
	Declare @IsSeniorHr bit=0
	--- END FPB ------
    SET @ServingQueueID = -1                                 -- Not Serving any Queue
    SET @NextQueueNumber = '----'                            -- Default No More Queue!
	-- changes 1
    BEGIN TRY
	
        BEGIN TRANSACTION 
		Declare @CurrentDay nvarchar(10)=Format(getdate(),'ddd')
		Declare @CurrentTime time(7)=convert(time,getdate())
		    select top 1 
			@Is_FPBBranch=Is_FPB,
			@Senior_Ratio=(case when @CurrentDay='Sat' then 
				case when @CurrentTime>=Senior_Sat_Start_Time and @CurrentTime<=Senior_Sat_End_Time then b.Sat_Senior_Ratio else b.Sat_Post_Senior_Ratio end
			else 
				case when @CurrentTime>=Senior_WeekDays_Start_Time and @CurrentTime<=Senior_WeekDays_End_Time then b.Senior_Ratio else b.Post_Senior_Ratio end 
			end),
			@NonSenior_Ratio=(case when @CurrentDay='Sat' then 
				case when @CurrentTime>=Senior_Sat_Start_Time and @CurrentTime<=Senior_Sat_End_Time then b.Sat_Non_Senior_Ratio else b.Sat_Post_Non_Senior_Ratio end 
			else 
				case when @CurrentTime>=Senior_WeekDays_Start_Time and @CurrentTime<=Senior_WeekDays_End_Time then b.Non_Senior_Ratio else b.Post_Non_Senior_Ratio end 
			end),
			@Senior_Ratio_Count=(case when @CurrentDay='Sat' then 
				case when @CurrentTime>=Senior_Sat_Start_Time and @CurrentTime<=Senior_Sat_End_Time then b.Sat_Senior_Ratio_Counter else b.Sat_Post_Senior_Ratio_Counter end
			else 
				case when @CurrentTime>=Senior_WeekDays_Start_Time and @CurrentTime<=Senior_WeekDays_End_Time then b.Senior_Ratio_Counter else b.Post_Senior_Ratio_Counter end 
			end),
		    @NonSenior_Ratio_Count=(case when @CurrentDay='Sat' then 
				case when @CurrentTime>=Senior_Sat_Start_Time and @CurrentTime<=Senior_Sat_End_Time then b.Sat_Non_Senior_Ratio_Counter else b.Sat_Post_Non_Senior_Ratio_Counter end
			else 
				case when @CurrentTime>=Senior_WeekDays_Start_Time and @CurrentTime<=Senior_WeekDays_End_Time then b.Non_Senior_Ratio_Counter else b.Post_Non_Senior_Ratio_Counter end 
			end),
			 @IsSeniorHr=(case when @CurrentDay='Sat' then 
				case when @CurrentTime>=Senior_Sat_Start_Time and @CurrentTime<=Senior_Sat_End_Time then 1 else 0 end
			else 
				case when @CurrentTime>=Senior_WeekDays_Start_Time and @CurrentTime<=Senior_WeekDays_End_Time then 1 else 0 end 
			end),
			@SeniorStartTime=(case when @CurrentDay='Sat' then Senior_Sat_Start_Time else Senior_WeekDays_Start_Time end),
		    @SeniorEndTime=(case when @CurrentDay='Sat' then Senior_Sat_End_Time else Senior_WeekDays_End_Time end)
			--@Senior_StartTime=(case when @CurrentDay='Sat' then DATEADD(day, DATEDIFF(day, 0, GETDATE()), cast(Senior_Sat_Start_Time as smalldatetime)) else DATEADD(day, DATEDIFF(day, 0, GETDATE()), cast(Senior_WeekDays_Start_Time as smalldatetime)) end),
			--@Senior_EndTime=(case when @CurrentDay='Sat' then DATEADD(day, DATEDIFF(day, 0, GETDATE()), cast(Senior_Sat_End_Time as smalldatetime)) else DATEADD(day, DATEDIFF(day, 0, GETDATE()), cast(Senior_WeekDays_End_Time as smalldatetime)) end)
			from Branch_Info b where id=@Branch_Info_ID

			select top 1 @Is_FPBCounter=Is_FPB_Counter from Counter_Info where id=@Counter_Info_ID

		    
            --SELECT TOP (1) @ServingQueueID = Q.[ID]
            --FROM [Queue_Info] Q WITH (UPDLOCK) 
            --WHERE Q.Branch_Info_ID = @Branch_Info_ID
            --    AND Q.Counter_Info_ID = @Counter_Info_ID
            --    AND Q.Queue_Status_ID = 2
            --    AND isnull(Q.Is_Appointment,0)=@Is_Appointment
			
			 if(@Is_Appointment=1)
			begin
				SELECT TOP (1) @ServingQueueID = Q.[ID]
				FROM [Queue_Info] Q WITH (UPDLOCK) 
				WHERE Q.Branch_Info_ID = @Branch_Info_ID
					AND Q.Counter_Info_ID = @Counter_Info_ID
					AND Q.Queue_Status_ID = 2
					AND isnull(Q.Is_Appointment,0)=@Is_Appointment
					order by AppTimeSlot asc
			end
		else
			begin
				SELECT TOP (1) @ServingQueueID = Q.[ID]
				FROM [Queue_Info] Q WITH (UPDLOCK) 
				WHERE Q.Branch_Info_ID = @Branch_Info_ID
					AND Q.Counter_Info_ID = @Counter_Info_ID
					AND Q.Queue_Status_ID = 2
					AND isnull(Q.Is_Appointment,0)=@Is_Appointment
			end

            IF @ServingQueueID > 0
            BEGIN
                
                SET @Ended_Timestamp = GETDATE()

                UPDATE Queue_Info                            -- Update the Queue Status!
                SET Queue_Status_ID = 5,
                    Ended_Timestamp = @Ended_Timestamp,
                    User_Info_ID = @User_Info_ID,
                    Counter_Info_ID = @Counter_Info_ID
                WHERE ID = @ServingQueueID 

                INSERT INTO [Queue_Audit] ([Queue_Info_ID], [Queue_Status_ID], [User_Info_ID], [QA_TimeStamp], [Counter_Info_ID])
                VALUES (@ServingQueueID, 5, @User_Info_ID, @Ended_Timestamp, @Counter_Info_ID);

            END
		   Declare @SeniorCount int=0;
		   IF(@Is_FPBBranch= 1 and @IS_FPBCounter=1 and  @Is_Appointment=0 and @Senior_Ratio>0) -- @CurrentTime>=@SeniorStartTime and @CurrentTime<=@SeniorEndTime and
				BEGIN
				
				   Declare @IsSeniorQueue bit
				   
				   select top 1 @IsSeniorQueue=s.Senior_Service from queue_info q 
				   inner join Service_Info s on s.id=q.Service_Info_ID
				   INNER JOIN [Counter_Service_Mapping] CSM 
										ON Q.[Service_Info_ID] = CSM.[Service_Info_ID] AND CSM.Counter_Info_ID=@Counter_Info_ID
				   where q.Branch_Info_ID=@Branch_Info_ID   and q.Queue_Status_ID=1 and isnull(Is_Appointment,0)=0
				   order by q.SortingKey, q.ID asc

				   if(@IsSeniorQueue=1)
						begin
							SELECT TOP (1) @NextQueueNumber = Q.[Queue_Number], @QueueID = Q.[ID], @ServiceInfoID = Q.Service_Info_ID, @Generated_Timestamp = Q.Generated_Timestamp, @Priority = CSM.Priority, @SortingKey = Q.SortingKey
						    FROM [Queue_Info] Q WITH (UPDLOCK) 
							INNER JOIN [Service_Info] S 
										ON Q.[Service_Info_ID] = S.[ID]
							INNER JOIN [Counter_Service_Mapping] CSM 
										ON Q.[Service_Info_ID] = CSM.[Service_Info_ID]
										AND CSM.Counter_Info_ID=@Counter_Info_ID
							WHERE Q.Branch_Info_ID = @Branch_Info_ID
							
							AND Q.Queue_Status_ID = 1
							AND Q.RM_User_Info_ID IS NULL
							and isnull(Is_Appointment,0)=0
							ORDER BY Q.SortingKey, Q.ID asc;
							set @Senior_Ratio_Count+=1
							if(@CurrentTime>=@SeniorStartTime and @CurrentTime<=@SeniorEndTime)
								begin
									if(@CurrentDay='Sat')update Branch_Info set Sat_Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID
									else update Branch_Info set Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID

									
								end
							else
								begin
									if(@CurrentDay='Sat')update Branch_Info set Sat_Post_Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID
									else update Branch_Info set Post_Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID
								end
						end
				   else
						begin
						
							SELECT @SeniorCount=Count(*)
							FROM [Queue_Info] Q WITH (UPDLOCK) 
							INNER JOIN [Service_Info] S 
										ON Q.[Service_Info_ID] = S.[ID]
							INNER JOIN [Counter_Service_Mapping] CSM 
										ON Q.[Service_Info_ID] = CSM.[Service_Info_ID] AND CSM.Counter_Info_ID=@Counter_Info_ID 
							WHERE Q.Branch_Info_ID = @Branch_Info_ID
							AND Q.Queue_Status_ID = 1 AND S.Senior_Service=1
							
							AND Q.RM_User_Info_ID IS NULL
							and isnull(Is_Appointment,0)=0
							and s.Senior_Service=1
							IF(@SeniorCount>0)
								BEGIN
									if(@Senior_Ratio_Count<@Senior_Ratio)
									begin
										SELECT TOP (1) @NextQueueNumber = Q.[Queue_Number], @QueueID = Q.[ID], @ServiceInfoID = Q.Service_Info_ID, @Generated_Timestamp = Q.Generated_Timestamp, @Priority = CSM.Priority, @SortingKey = Q.SortingKey
										FROM [Queue_Info] Q WITH (UPDLOCK) 
										INNER JOIN [Service_Info] S 
													ON Q.[Service_Info_ID] = S.[ID]
										INNER JOIN [Counter_Service_Mapping] CSM 
													ON Q.[Service_Info_ID] = CSM.[Service_Info_ID]
													AND CSM.Counter_Info_ID=@Counter_Info_ID
										WHERE Q.Branch_Info_ID = @Branch_Info_ID
										
										AND Q.Queue_Status_ID = 1
										AND Q.RM_User_Info_ID IS NULL
										AND S.Senior_Service=1
										and isnull(Is_Appointment,0)=0
										ORDER BY Q.SortingKey, Q.ID asc;

										set @Senior_Ratio_Count+=1
										if(@CurrentTime>=@SeniorStartTime and @CurrentTime<=@SeniorEndTime)
											begin
												if(@CurrentDay='Sat')update Branch_Info set Sat_Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID
												else update Branch_Info set Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID
									
											end
										else
											begin
												if(@CurrentDay='Sat')update Branch_Info set Sat_Post_Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID
												else update Branch_Info set Post_Senior_Ratio_Counter=@Senior_Ratio_Count where id=@Branch_Info_ID

											end
										
									end
								 else if(@NonSenior_Ratio_Count<@NonSenior_Ratio)
									begin
										SELECT TOP (1) @NextQueueNumber = Q.[Queue_Number], @QueueID = Q.[ID], @ServiceInfoID = Q.Service_Info_ID, @Generated_Timestamp = Q.Generated_Timestamp, @Priority = CSM.Priority, @SortingKey = Q.SortingKey
										FROM [Queue_Info] Q WITH (UPDLOCK) 
										INNER JOIN [Service_Info] S 
													ON Q.[Service_Info_ID] = S.[ID]
										INNER JOIN [Counter_Service_Mapping] CSM 
													ON Q.[Service_Info_ID] = CSM.[Service_Info_ID]
													AND CSM.Counter_Info_ID=@Counter_Info_ID
										WHERE Q.Branch_Info_ID = @Branch_Info_ID
										
										AND Q.Queue_Status_ID = 1
										AND S.Senior_Service=0
										AND Q.RM_User_Info_ID IS NULL
										and isnull(Is_Appointment,0)=0
										ORDER BY Q.SortingKey, Q.ID asc;
										set @NonSenior_Ratio_Count+=1;
										
										if(@NonSenior_Ratio_Count=@NonSenior_Ratio and @Senior_Ratio_Count>=@Senior_Ratio)
											begin
											  set @NonSenior_Ratio_Count=0;
											  set @Senior_Ratio_Count=0;
											end

										if(@CurrentTime>=@SeniorStartTime and @CurrentTime<=@SeniorEndTime)
											begin
												if(@CurrentDay='Sat')update Branch_Info set Sat_Senior_Ratio_Counter=@Senior_Ratio_Count,Sat_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
												else update Branch_Info set Senior_Ratio_Counter=@Senior_Ratio_Count,Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
											end
										else
											begin
												if(@CurrentDay='Sat')update Branch_Info set Sat_Post_Senior_Ratio_Counter=@Senior_Ratio_Count,Sat_Post_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
												else update Branch_Info set Post_Senior_Ratio_Counter=@Senior_Ratio_Count,Post_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID	
											end
									end
								else
									begin
										SELECT TOP (1) @NextQueueNumber = Q.[Queue_Number], @QueueID = Q.[ID], @ServiceInfoID = Q.Service_Info_ID, @Generated_Timestamp = Q.Generated_Timestamp, @Priority = CSM.Priority, @SortingKey = Q.SortingKey
										FROM [Queue_Info] Q WITH (UPDLOCK) 
										INNER JOIN [Service_Info] S 
													ON Q.[Service_Info_ID] = S.[ID]
										INNER JOIN [Counter_Service_Mapping] CSM 
													ON Q.[Service_Info_ID] = CSM.[Service_Info_ID]
													AND CSM.Counter_Info_ID=@Counter_Info_ID
										WHERE Q.Branch_Info_ID = @Branch_Info_ID
										
										AND Q.Queue_Status_ID = 1
										AND Q.RM_User_Info_ID IS NULL
										AND S.Senior_Service=1
										and isnull(Is_Appointment,0)=0
										ORDER BY Q.SortingKey, Q.ID asc;
										set @NonSenior_Ratio_Count=0;
										set @Senior_Ratio_Count=1;
										if(@CurrentTime>=@SeniorStartTime and @CurrentTime<=@SeniorEndTime)
											begin
												if(@CurrentDay='Sat') update Branch_Info set Sat_Senior_Ratio_Counter=@Senior_Ratio_Count,Sat_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
												else update Branch_Info set Senior_Ratio_Counter=@Senior_Ratio_Count,Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
												
											end
										else
											begin
												if(@CurrentDay='Sat')update Branch_Info set Sat_Post_Senior_Ratio_Counter=@Senior_Ratio_Count,Sat_Post_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
												else update Branch_Info set Post_Senior_Ratio_Counter=@Senior_Ratio_Count,Post_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
											end
										
									end
								END
							ELSE
								BEGIN
									SELECT TOP (1) @NextQueueNumber = Q.[Queue_Number], @QueueID = Q.[ID], @ServiceInfoID = Q.Service_Info_ID, @Generated_Timestamp = Q.Generated_Timestamp, @Priority = CSM.Priority, @SortingKey = Q.SortingKey
									FROM [Queue_Info] Q WITH (UPDLOCK) 
									INNER JOIN [Service_Info] S 
												ON Q.[Service_Info_ID] = S.[ID]
									INNER JOIN [Counter_Service_Mapping] CSM 
												ON Q.[Service_Info_ID] = CSM.[Service_Info_ID] AND CSM.Counter_Info_ID=@Counter_Info_ID
									WHERE Q.Branch_Info_ID = @Branch_Info_ID
									
									AND Q.Queue_Status_ID = 1
									AND Q.RM_User_Info_ID IS NULL
									and isnull(Is_Appointment,0)=0
									ORDER BY Q.SortingKey, Q.ID asc;
									
									if(@CurrentTime>=@SeniorStartTime and @CurrentTime<=@SeniorEndTime)
											begin
												if(@CurrentDay='Sat')update Branch_Info set Sat_Senior_Ratio_Counter=0,Sat_Non_Senior_Ratio_Counter=0 where id=@Branch_Info_ID
												else update Branch_Info set Senior_Ratio_Counter=0,Non_Senior_Ratio_Counter=0 where id=@Branch_Info_ID
											end
										else
											begin
											if(@CurrentDay='Sat')update Branch_Info set Sat_Post_Senior_Ratio_Counter=0,Sat_Post_Non_Senior_Ratio_Counter=0 where id=@Branch_Info_ID
												else update Branch_Info set Post_Senior_Ratio_Counter=0,Post_Non_Senior_Ratio_Counter=0 where id=@Branch_Info_ID
											end
								END
						end  

					--if(@NonSenior_Ratio_Count>=@NonSenior_Ratio and @Senior_Ratio_Count>=@Senior_Ratio)
					--	begin
					--		set @NonSenior_Ratio_Count=0;
					--		set @Senior_Ratio_Count=0;
					--	end

					--if(@CurrentTime>=@SeniorStartTime and @CurrentTime<=@SeniorEndTime)
					--	begin
					--		if(@CurrentDay='Sat')update Branch_Info set Sat_Senior_Ratio_Counter=@Senior_Ratio_Count,Sat_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
					--		else update Branch_Info set Senior_Ratio_Counter=@Senior_Ratio_Count,Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
					--	end
					--else
					--	begin
					--		if(@CurrentDay='Sat')update Branch_Info set Sat_Post_Senior_Ratio_Counter=@Senior_Ratio_Count,Sat_Post_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID
					--		else update Branch_Info set Post_Senior_Ratio_Counter=@Senior_Ratio_Count,Post_Non_Senior_Ratio_Counter=@NonSenior_Ratio_Count where id=@Branch_Info_ID	
					--	end
				END
			ELSE
				BEGIN
				    if(@Is_Appointment=1)
						BEGIN
							SELECT TOP (1) @NextQueueNumber = Q.[Queue_Number], @QueueID = Q.[ID], @ServiceInfoID = Q.Service_Info_ID, @Generated_Timestamp = Q.Generated_Timestamp, @Priority = CSM.Priority, @SortingKey = Q.SortingKey
							FROM [Queue_Info] Q WITH (UPDLOCK) 
							INNER JOIN [Service_Info] S 
										ON Q.[Service_Info_ID] = S.[ID]
							INNER JOIN [Counter_Service_Mapping] CSM 
										ON Q.[Service_Info_ID] = CSM.[Service_Info_ID]
										AND CSM.Counter_Info_ID = @Counter_Info_ID
							WHERE Q.Branch_Info_ID = @Branch_Info_ID
							
							AND Q.Queue_Status_ID = 1
							AND Q.RM_User_Info_ID IS NULL
							AND isnull(Q.Is_Appointment,0)=@Is_Appointment
							ORDER BY AppTimeSlot asc;
						
						END
					ELSE
						BEGIN
							SELECT TOP (1) @NextQueueNumber = Q.[Queue_Number], @QueueID = Q.[ID], @ServiceInfoID = Q.Service_Info_ID, @Generated_Timestamp = Q.Generated_Timestamp, @Priority = CSM.Priority, @SortingKey = Q.SortingKey
							FROM [Queue_Info] Q WITH (UPDLOCK) 
							INNER JOIN [Service_Info] S 
										ON Q.[Service_Info_ID] = S.[ID]
							INNER JOIN [Counter_Service_Mapping] CSM 
										ON Q.[Service_Info_ID] = CSM.[Service_Info_ID]
										AND CSM.Counter_Info_ID = @Counter_Info_ID
							WHERE Q.Branch_Info_ID = @Branch_Info_ID
							AND Q.Queue_Status_ID = 1
							AND Q.RM_User_Info_ID IS NULL
							AND isnull(Q.Is_Appointment,0)=@Is_Appointment
							ORDER BY CSM.Priority, Q.SortingKey, Q.ID;
						END
					
					
				END
            

            IF @NextQueueNumber != '----'
            BEGIN

                SET @Called_Timestamp = GETDATE()

                UPDATE Queue_Info                            -- Update the Queue Status!
                SET Queue_Status_ID = 2,
                    Called_Timestamp = @Called_Timestamp,
                    User_Info_ID = @User_Info_ID,
                    Counter_Info_ID = @Counter_Info_ID
                WHERE ID = @QueueID

                INSERT INTO [Queue_Audit] ([Queue_Info_ID], [Queue_Status_ID], [User_Info_ID], [QA_TimeStamp], [Counter_Info_ID])
                VALUES (@QueueID, 2, @User_Info_ID, @Called_Timestamp, @Counter_Info_ID);

                DELETE QDR
                FROM Queue_Display_Record QDR, Queue_Info Q
                WHERE QDR.Queue_Info_ID = Q.ID AND Q.Branch_Info_ID = @Branch_Info_ID AND Q.Queue_Number = @NextQueueNumber;

                INSERT INTO [Queue_Display_Record] ([Display_Info_ID], [Queue_Info_ID], [Counter_Info_ID], [Display_Timestamp], [Blink])
                SELECT M.Display_Info_ID, @QueueID, @Counter_Info_ID, @Called_Timestamp, 1
                FROM Counter_Display_Mapping M (nolock) WHERE M.Counter_Info_ID = @Counter_Info_ID

            END
        
        COMMIT TRANSACTION

    END TRY

    BEGIN CATCH
        ROLLBACK TRANSACTION
        SET @QueueID = 0
        SET @NextQueueNumber = '----'
    END CATCH

    SELECT @QueueID AS 'QueueID', @NextQueueNumber AS 'NextQueueNumber', @Called_Timestamp AS 'CalledTime', @Priority AS 'Priority', @SortingKey AS 'SortingKey'

END