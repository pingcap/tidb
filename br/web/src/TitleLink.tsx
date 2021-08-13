// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

import MuiLink from '@material-ui/core/Link';
import * as React from 'react';
import { Link } from 'react-router-dom';


interface Props {
    className: string
}

export default class InfoButton extends React.Component<Props> {
    render() {
        return (
            <div className={this.props.className}>
                <MuiLink color='inherit' variant='h6' component={Link} to='progress'>
                    TiDB Lightning
                </MuiLink>
            </div>
        );
    }
}
