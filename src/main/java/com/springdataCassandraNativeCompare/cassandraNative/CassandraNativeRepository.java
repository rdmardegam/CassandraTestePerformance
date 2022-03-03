package com.springdataCassandraNativeCompare.cassandraNative;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.springdataCassandraNativeCompare.springData.entity.DummyItem;

import lombok.extern.log4j.Log4j2;

/**
 * @author Fatih Yıldızlı
 */
@Repository
@Log4j2
public class CassandraNativeRepository {

    //private CqlSession cqlSession = CassandraConfig.getCqlSession();
	
	@Autowired
    private CqlSession cqlSession;

    private static PreparedStatement selectAllStatement = null;
    private static PreparedStatement insertAllStatement = null;
    private static PreparedStatement updateAllStatement = null;
    private static PreparedStatement deleteAllStatement = null;

    public List<DummyItem> selectAll() {
        try {
        
        	if (selectAllStatement == null) {
                selectAllStatement();
            }
        	
        	//System.out.println(cqlSession.getContext().getConfig());
        	log.info("INICIANDO PESQUISA");
        	List<DummyItem> response = new ArrayList<>();
            ResultSet resultSet = cqlSession.execute(selectAllStatement.bind());
            
        	//ResultSet resultSet = cqlSession.execute("Select id, column_1, column_2   from local.dummy");
            
            for (Row row : resultSet) {
                DummyItem item = new DummyItem( row.getString("num_cpf_cnpj"), row.getInt("mes_ano_lancamento"), 
                								row.getLocalDate("dat_autr"), row.getString("cod_chav_lancamento"),
                							 	row.getString("codigo_moeda_origem"), row.getDouble("valor"));

                response.add(item);
            }


            return response;
        } catch (Exception ex) {
            ex.printStackTrace();

        }

        return null;

    }

    private void selectAllStatement() {
        try {
            Select selectStatement = selectFrom("local",
                    "dummy").columns("id", "column_2", "column_1").all();
            selectAllStatement = cqlSession.prepare(selectStatement.build());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    
    /************/
    private static PreparedStatement selectFilterStatement = null;
    
    @Async("threadPoolExecutor")
    public List<DummyItem> selectFilter(String num_cpf_cnpj ,int mes_ano_lancamento,LocalDate dataInicio, LocalDate dataFim) {
        try {
        
        	if (selectFilterStatement == null) {
        		 try {
        	            Select select = selectFrom("local","dummy")
        	            		.columns("num_cpf_cnpj",
        	            				"mes_ano_lancamento", 
        	            				"cod_chav_lancamento" ,
        	            				"dat_autr" , 
        	            				"codigo_moeda_origem" , 
        	            				"valor")
        	            		
        	            		.whereColumn("num_cpf_cnpj").isEqualTo(bindMarker())
        	            		.whereColumn("mes_ano_lancamento").isEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isGreaterThanOrEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isLessThanOrEqualTo(bindMarker())
        	            		;
        	            
        	            selectFilterStatement = cqlSession.prepare(select.build());

        	        } catch (Exception ex) {
        	            ex.printStackTrace();
        	        }
            }
        	
        	//System.out.println(cqlSession.getContext().getConfig());
        	//log.info("INICIANDO PESQUISA");
        	
        	List<DummyItem> response = new ArrayList<>();
            
            BoundStatement boundStatement = selectFilterStatement.bind()
            								.setString(0, num_cpf_cnpj)
            								.setInt(1, mes_ano_lancamento)
            								.setLocalDate(2, dataInicio)
            								.setLocalDate(3, dataFim);
        	//BoundStatement boundStatement = selectFilterStatement.bind(num_cpf_cnpj, mes_ano_lancamento, dataInicio, dataFim);
            
            
            
            								
            ResultSet resultSet = cqlSession.execute(boundStatement);
            
            for (Row row : resultSet) {
            	DummyItem item = new DummyItem( row.getString("num_cpf_cnpj"), row.getInt("mes_ano_lancamento"), 
						row.getLocalDate("dat_autr"), row.getString("cod_chav_lancamento"),
					 	row.getString("codigo_moeda_origem"), row.getDouble("valor"));

                response.add(item);
            }


            return response;
        } catch (Exception ex) {
            ex.printStackTrace();

        }

        return null;

    }
    
    
    
    @Async("threadPoolExecutor")
    public CompletableFuture<List<DummyItem>> selectFilterAsync(String num_cpf_cnpj ,int mes_ano_lancamento, LocalDate dataInicio, LocalDate dataFim) {
    	log.info("CHAMANDO selectFilterAsync - " + mes_ano_lancamento);
    	
    	System.out.println("PROTOCOLO= " +  cqlSession.getContext().getProtocolVersion());
    	
    	List<DummyItem> response = new ArrayList<>();
    	try {
        
        	if (selectFilterStatement == null) {
        		 try {
        	            Select select = selectFrom("local","dummy")
        	            		.columns("num_cpf_cnpj",
        	            				"mes_ano_lancamento", 
        	            				"cod_chav_lancamento" ,
        	            				"dat_autr" , 
        	            				"codigo_moeda_origem" , 
        	            				"valor")
        	            		
        	            		.whereColumn("num_cpf_cnpj").isEqualTo(bindMarker())
        	            		.whereColumn("mes_ano_lancamento").isEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isGreaterThanOrEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isLessThanOrEqualTo(bindMarker())
        	            		;
        	            
        	            selectFilterStatement = cqlSession.prepare(select.build());

        	        } catch (Exception ex) {
        	            ex.printStackTrace();
        	        }
            }
        	
        	//System.out.println(cqlSession.getContext().getConfig());
        	//log.info("INICIANDO PESQUISA");
        	
        	
            
            BoundStatement boundStatement = selectFilterStatement.bind()
            								.setString(0, num_cpf_cnpj)
            								.setInt(1, mes_ano_lancamento)
            								.setLocalDate(2, dataInicio)
            								.setLocalDate(3, dataFim);
        	//BoundStatement boundStatement = selectFilterStatement.bind(num_cpf_cnpj, mes_ano_lancamento, dataInicio, dataFim);
            
            
            
            								
            ResultSet resultSet = cqlSession.execute(boundStatement);
            
            for (Row row : resultSet) {
            	DummyItem item = new DummyItem( row.getString("num_cpf_cnpj"), row.getInt("mes_ano_lancamento"), 
						row.getLocalDate("dat_autr"), row.getString("cod_chav_lancamento"),
					 	row.getString("codigo_moeda_origem"), row.getDouble("valor"));

                response.add(item);
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();

        }

    	return CompletableFuture.completedFuture(response);
    }


    
    /***********/
    
    
    
    
    public void insertAll(long id) {
        try {

            if (insertAllStatement == null) {
                insertAllStatement();
            }
            BoundStatement boundStatement =
                    insertAllStatement.bind(id, "fatih", "yıldızlı"
                    );

            cqlSession.executeAsync(boundStatement);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void insertAllStatement() {
        SimpleStatement insertStatement =
                insertInto("local",
                        "dummy")
                        .value("id", bindMarker())
                        .value("column_1", bindMarker())
                        .value("column_2", bindMarker()).build();
        insertAllStatement = cqlSession.prepare(insertStatement);
    }

    public void updateAll(long id) {
        try {

            if (updateAllStatement == null) {
                updateAllStatement();
            }
            BoundStatement boundStatement = updateAllStatement.bind("FY", "yıldızlı"
                    , id);

            cqlSession.executeAsync(boundStatement);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void updateAllStatement() {
        Update updateStatement =
                update("local",
                        "dummy")
                        .setColumn("column_2", bindMarker())
                        .setColumn("column_1", bindMarker())
                        .whereColumn("id").isEqualTo(bindMarker());
        updateAllStatement =
                cqlSession.prepare(updateStatement.build());
    }

    public void deleteAll(long id) {
        try {

            if (deleteAllStatement == null) {
                deleteAllStatement();
            }
            BoundStatement boundStatement = deleteAllStatement.bind(id);

            cqlSession.executeAsync(boundStatement);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void deleteAllStatement() {
        Delete deleteStatement = deleteFrom("local",
                "dummy").whereColumn("id").isEqualTo(bindMarker());
        deleteAllStatement =
                cqlSession.prepare(deleteStatement.build());
    }

}
