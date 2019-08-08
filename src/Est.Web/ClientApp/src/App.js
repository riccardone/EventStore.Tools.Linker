import React from 'react';
import { Route } from 'react-router';
import Layout from './components/Layout';
import Home from './components/Home';
import Replicas from './components/Replicas';

export default () => (
  <Layout>
    <Route exact path='/' component={Home} />
    <Route path='/replicas' component={Replicas} />
  </Layout>
);
