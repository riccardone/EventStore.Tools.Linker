import React from 'react';
import { bindActionCreators } from 'redux';
import { connect } from 'react-redux';
import { actionCreators } from '../store/Replicas';

const Replicas = props => (
    <div>
        <h1>EventStore Replica's</h1>

        <p>Cross Cluster Replication</p>
        <p>TODO...</p>
    </div>
);

export default connect(
    state => state.counter,
    dispatch => bindActionCreators(actionCreators, dispatch)
)(Replicas);
