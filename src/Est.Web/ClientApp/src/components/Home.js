import React from 'react';
import { connect } from 'react-redux';

const Home = props => (
  <div>
    <h1>EventStore Tools</h1>
    <p>Toolset to support EventStore</p>
  </div>
);

export default connect()(Home);
