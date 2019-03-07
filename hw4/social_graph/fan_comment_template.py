from py2neo import Graph, NodeMatcher, Node, Relationship
import json
from utils import utils as ut
import uuid


class FanGraph(object):
    """
    This object provides a set of helper methods for creating and retrieving Nodes and relationship from
    a Neo4j database.
    """

    # Connects to the DB and sets a Graph instance variable.
    # Also creates a NodeMatcher, which is a py2neo class.
    def __init__(self, auth, host, port, secure=False, ):
        self._graph = Graph(secure=secure,
                            bolt=True,
                            auth=auth,
                            host=host,
                            port=port)
        self._node_matcher = NodeMatcher(self._graph)

    def run_match(self, labels=None, properties=None):
        """
        Uses a NodeMatcher to find a node matching a "template."
        :param labels: A list of labels that the node must have.
        :param properties: A parameter list of the form prop1=value1, prop2=value2, ...
        :return: An array of Node objects matching the pattern.
        """
        # ut.debug_message("Labels = ", labels)
        # ut.debug_message("Properties = ", json.dumps(properties))

        if labels is not None and properties is not None:
            result = self._node_matcher.match(labels, **properties)
        elif labels is not None and properties is None:
            result = self._node_matcher.match(labels)
        elif labels is None and properties is not None:
            result = self._node_matcher.match(**properties)
        else:
            raise ValueError("Invalid request. Labels and properties cannot both be None.")

        # Convert NodeMatch data into a simple list of Nodes.
        full_result = []
        for r in result:
            full_result.append(r)

        return full_result

    def find_nodes_by_template(self, tmp):
        """

        :param tmp: A template defining the label and properties for Nodes to return. An
         example is { "label": "Fan", "template" { "last_name": "Ferguson", "first_name": "Donald" }}
        :return: A list of Nodes matching the template.
        """
        labels = tmp.get('label')
        props = tmp.get("template")
        result = self.run_match(labels=labels, properties=props)
        return result

    # Create and save a new node for  a 'Fan.'
    def create_fan(self, uni, last_name, first_name):
        n = Node("Fan", uni=uni, last_name=last_name, first_name=first_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)

    # Given a UNI, return the node for the Fan.
    def get_fan(self, uni):
        n = self.find_nodes_by_template({"label": "Fan", "template": {"uni": uni}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_player(self, player_id, last_name, first_name):
        n = Node("Player", player_id=player_id, last_name=last_name, first_name=first_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)
        return n

    def get_player(self, player_id):
        n = self.find_nodes_by_template({"label": "Player", "template": {"player_id": player_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_team(self, team_id, team_name):
        n = Node("Team", team_id=team_id, team_name=team_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)
        return n

    def get_team(self, team_id):
        n = self.find_nodes_by_template({"label": "Team", "template": {"team_id": team_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_supports(self, uni, team_id):
        """
        Create a SUPPORTS relationship from a Fan to a Team.
        :param uni: The UNI for a fan.
        :param team_id: An ID for a team.
        :return: The created SUPPORTS relationship from the Fan to the Team
        """
        f = self.get_fan(uni)
        t = self.get_team(team_id)
        r = Relationship(f, "SUPPORTS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)
        return r

    # Create an APPEARED relationship from a player to a Team
    def create_appearance(self, player_id, team_id):
        try:
            f = self.get_player(player_id)
            t = self.get_team(team_id)
            r = Relationship(f, "APPEARED", t)
            tx = self._graph.begin(autocommit=True)
            tx.create(r)
        except Exception as e:
            print("create_appearances: exception = ", e)

    # Create a FOLLOWS relationship from a Fan to another Fan.
    def create_follows(self, follower, followed):
        f = self.get_fan(follower)
        t = self.get_fan(followed)
        r = Relationship(f, "FOLLOWS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)

    # Create a COMMENT ON relationship from a comment to a player.
    def create_comment_on_player(self, comment_id, player_id):
        c = self.get_comment(comment_id)
        p = self.get_player(player_id)
        if c is None:
            raise ValueError("Comment not exist")
        if p is None:
            raise ValueError("Player not exist")

        r = Relationship(c, "COMMENT_ON", p)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)

    # Create a COMMENT ON relationship from a comment to a team.
    def create_comment_on_team(self, comment_id, team_id):
        c = self.get_comment(comment_id)
        t = self.get_team(team_id)
        if c is None:
            raise ValueError("Comment not exist")
        if t is None:
            raise ValueError("Team not exist")
        r = Relationship(c, "COMMENT_ON", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)

    # create a RESPONSE TO relationship from a sub-comment to a comment or a response
    def create_response(self, ori_comment_id, sub_comment_id):
        oc = self.get_comment(ori_comment_id)
        sc = self.get_comment(sub_comment_id)
        r = Relationship(sc, "RESPONSE_TO", oc)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)

    def create_comment(self, uni, comment, team_id=None, player_id=None):
        """
        Creates a comment
        :param uni: The UNI for the Fan making the comment.
        :param comment: A simple string.
        :param team_id: A valid team ID or None. team_id and player_id cannot BOTH be None.
        :param player_id: A valid player ID or None
        :return: The Node representing the comment.
        """
        if not player_id and not team_id:
            return None

        # create node for comment
        comment_id = str(uuid.uuid4())
        n = Node("Comment", comment_id=comment_id, comment=comment)
        tx = self._graph.begin()
        tx.create(n)
        # create a COMMENT relationship from fan to comment
        f = self.get_fan(uni)
        if f is None:
            raise ValueError("Uni not exist")

        r = Relationship(f, "COMMENT_FROM", n)
        tx.create(r)
        tx.commit()

        if player_id:
            # create a COMMENT_ON relationship from player to comment
            self.create_comment_on_player(comment_id, player_id)
        if team_id:
            # create a COMMENT_ON relationship from team to comment
            self.create_comment_on_team(comment_id, team_id)
        return n

    def create_sub_comment(self, uni, origin_comment_id, comment):
        """
        Create a sub-comment (response to a comment or response) and links with parent in thread.
        :param uni: ID of the Fan making the comment.
        :param origin_comment_id: Id of the comment to which this is a response.
        :param comment: Comment string
        :return: Created comment.
        """
        # create node for sub-comment
        sub_comment_id = str(uuid.uuid4())
        n = Node("Comment", comment_id=sub_comment_id, comment=comment)
        tx = self._graph.begin()
        tx.create(n)
        # create a RESPONSE FROM relationship from fan to subcomment
        f = self.get_fan(uni)
        r = Relationship(f, "RESPONSE_FROM", n)
        tx.create(r)
        tx.commit()

        # create a RESPONSE TO relationship from a sub-comment to a comment or a response
        self.create_response(origin_comment_id, sub_comment_id)
        return n

    def get_comment(self, comment_id):
        n = self.find_nodes_by_template({"label": "Comment", "template": {"comment_id": comment_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None
        return n

    def get_player_comments(self, player_id):
        """
        Gets all of the comments associated with a player, all of the comments on the comment and comments
        on the comments, etc. Also returns the Nodes for people making the comments.
        :param player_id: ID of the player.
        :return: Graph containing comment, comment streams and commenters.
        """
        res = {}
        p = self.get_player(player_id)
        # find all comments on player
        tx = self._graph
        q = "MATCH (f:Fan)-[r2]-(c)-[r1:COMMENT_ON]->(p:Player {player_id:{pid}}) return p,r1,c,r2,f"
        r = tx.run(q,pid = player_id)
        return r
        # com_rels = tx.match((None, p), r_type="COMMENT_ON")
        # res["player"] = []
        # for com_rel in com_rels:
        #     r_dict = {}
        #     c = com_rel.start_node
        #     f = tx.match((None, c), r_type="COMMENT_FROM")
        #     r_dict["comment_id"] = c['comment_id']
        #     r_dict["comment"] = c['comment']
        #     r_dict["fan"] = f.first().start_node['uni']
        #     responses = self.dig_comment(c)
        #     if len(responses) > 0:
        #         r_dict["responses"] = responses
        #     res["player"].append(r_dict)
        # return ut.safe_dumps(res)

    def get_team_comments(self, team_id):
        """
        Gets all of the comments associated with a teams, all of the comments on the comment and comments
        on the comments, etc. Also returns the Nodes for people making the comments.
        :param player_id: ID of the team.
        :return: Graph containing comment, comment streams and commenters.
        """
        res = {}
        t = self.get_team(team_id)
        # find all comments on team
        tx = self._graph
        q = "MATCH (f:Fan)-[r2]-(c)-[r1:COMMENT_ON]->(t:Team {team_id:{tid}}) return t,r1,c,r2,f"
        r = tx.run(q, tid=team_id)
        return r
        # com_rels = tx.match((None, t), r_type="COMMENT_ON")
        # res["team"] = []
        # for com_rel in com_rels:
        #     r_dict = {}
        #     c = com_rel.start_node
        #     f = tx.match((None, c), r_type="COMMENT_FROM")
        #     r_dict["comment_id"] = c['comment_id']
        #     r_dict["comment"] = c['comment']
        #     r_dict["fan"] = f.first().start_node['uni']
        #     responses = self.dig_comment(c)
        #     if len(responses) > 0:
        #         r_dict["responses"] = responses
        #     res["team"].append(r_dict)
        # return ut.safe_dumps(res)

    def dig_comment(self, comment):
        tx = self._graph
        res = tx.match((None, comment), r_type="RESPONSE_TO")
        subs = []
        if res is not None and len(res) > 0:
            for r in res:
                r_dict = {}
                c = r.start_node
                f = tx.match((None, c), r_type="RESPONSE_FROM")
                r_dict["comment_id"] = c['comment_id']
                r_dict["comment"] = c['comment']
                r_dict["fan"] = f.first().start_node['uni']
                responses = self.dig_comment(c)
                if len(responses) > 0:
                    r_dict["responses"] = responses
                subs.append(r_dict)
        return subs

"""
bryankr01   CHN
scherma01   WAS
abreujo02   CHA
ortizda01   BOS
jeterde01   NYA
"""
