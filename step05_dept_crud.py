import cx_Oracle

def dept01_create():
    # 연결이 되는 경우만 테이블 drop과 create 실행. drop이 실패하더라도 create 실행
    try:
        conn = cx_Oracle.connect(user="SCOTT", password="TIGER", dsn="xe")
        cur = conn.cursor()
        print('conn, cur 객체 생성 성공')

        try:
            cur.execute('drop table dept01')
            print('테이블 Drop 성공')
        except Exception as e:
            print(e)

        try:
            cur.execute('create table dept01 as select deptno, dname from dept')
            print('테이블 Create 성공')
        except Exception as e:
            print(e)
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        print(e)
        

def dept01_select(deptno):

    try:
        conn = cx_Oracle.connect(user="SCOTT", password="TIGER", dsn="xe")
        cur = conn.cursor()
        print('conn, cur 객체 생성 성공')

        try:
            cur.execute("select * from dept01 where deptno = :deptno", deptno = deptno)
            rows = cur.fetchall() 
            if len(rows) == 0: # 조회된 데이터가 없을 경우 exeception 실행
                raise Exception()
            for row in rows:
                print(row)
            print('테이블 조회 성공')
        except Exception as e:
            print(e)
            print('테이블 조회 실패')
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        print(e)


def dept01_insert(deptno,dname):
    # 해당 dept가 존재하는 경우 insert 실행 안함
    try:
        conn = cx_Oracle.connect(user="SCOTT", password="TIGER", dsn="xe")
        cur = conn.cursor()
        print('conn, cur 객체 생성 성공')

        try:
            cur.execute("select * from dept01 where deptno = :deptno", deptno = deptno)
            rows = cur.fetchall() 
            if len(rows) > 0: # 조회된 데이터가 있을 경우 exeception 실행
                raise Exception()
            
            cur.execute("insert into dept01 values(:deptno, :dname)", deptno = deptno, dname = dname )
            conn.commit()
            print('해당 부서번호 생성 성공')
        except Exception as e:
            print(e)
            print('해당 부서번호 생성 실패')
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        print(e)
        

def dept01_update(deptno,dname):
    # 업데이트할 부서번호의 유무 확인 후 업데이트 진행
    try:
        conn = cx_Oracle.connect(user="SCOTT", password="TIGER", dsn="xe")
        cur = conn.cursor()
        print('conn, cur 객체 생성 성공')

        try:
            cur.execute("select * from dept01 where deptno = :deptno", deptno = deptno)
            rows = cur.fetchall() 
            if len(rows) == 0: # 조회된 데이터가 없을 경우 exeception 실행
                raise Exception()

            cur.execute("update dept01 set dname = :dname where deptno = :deptno ", deptno = deptno, dname = dname )
            conn.commit()
            print('해당 부서번호 업데이트 성공')
        except Exception as e:
            print(e)
            print('해당 부서번호 업데이트 실패')
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        print(e)


def dept01_delete(deptno):
    # 삭제할 부서번호의 유무 확인 후 삭제 진행
    try:
        conn = cx_Oracle.connect(user="SCOTT", password="TIGER", dsn="xe")
        cur = conn.cursor()
        print('conn, cur 객체 생성 성공')
        try:
            cur.execute("select * from dept01 where deptno = :deptno", deptno = deptno)
            rows = cur.fetchall() 
            if len(rows) == 0: # 조회된 데이터가 없을 경우 exeception 실행
                raise Exception()
            
            cur.execute("delete from dept01 where deptno = :deptno", deptno = deptno)
            conn.commit()
            print('해당 부서번호 삭제 성공')
        except Exception as e:
            print(e)
            print('해당 부서번호 삭제 실패')
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    # dept01_create()
    # dept01_select(50)
    
    # dept01_insert(50,'TEACHER')
    # dept01_select(50)

    # dept01_update(50,'STUDENT')
    # dept01_select(50)

    # dept01_delete(50)
    # dept01_select(50)

    pass