package com.datastax.samples.objectmapping;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

/**
 * Definition of operation for mapping.
 */
@Mapper
public interface CommentDaoMapper {

    @DaoFactory
    CommentDao commentDao();

    static MapperBuilder<CommentDaoMapper> builder(CqlSession session) {
        return new CommentDaoMapperBuilder(session);
    }
}

